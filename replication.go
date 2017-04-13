package pq

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type ReplicationEvent struct {
	LogPos  LogPos
	Payload []byte
}

type ReplicationConn struct {
	cn             *conn
	streaming      bool
	commitInterval time.Duration
	writingLock    sync.Mutex
	closeLock      sync.RWMutex
	rbuf           *readBuf
	wbuf           *writeBuf

	events chan *ReplicationEvent

	writeLogPos LogPos
	flushLogPos LogPos

	closeChan  chan struct{}
	closedChan chan struct{}
	closed     bool
	err        error
}

func NewReplicationConnection(connString string) (*ReplicationConn, error) {
	if !strings.Contains(connString, "replication=") {
		connString = fmt.Sprintf("%s replication=database", connString)
	}

	cn, err := Open(connString)
	if err != nil {
		return nil, err
	}

	return &ReplicationConn{
		cn:   cn.(*conn),
		rbuf: &readBuf{},
		wbuf: &writeBuf{
			buf: make([]byte, 512),
			pos: 1,
		},
		events:     make(chan *ReplicationEvent, 0),
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}, 2),
	}, nil
}

type IdentifySystemMsg struct {
	// systemid - The unique system identifier identifying the cluster.
	SystemId string

	// timeline - Current TimelineID.
	Timeline int64

	// xlogpos - Current xlog write location.
	XLogPos LogPos

	// dbname - Database connected to.
	DBName string
}

type CreateLogicalReplicationSlotResult struct {
	SlotName     string
	XLogPos      LogPos
	SnapshotName string
	OutputPlugin string
}

func (r *ReplicationConn) IdentifySystem() (*IdentifySystemMsg, error) {
	if r.streaming {
		return nil, errors.New("replication stream already running")
	}

	if r.closed {
		return nil, errors.New("can't run on already closed connection")
	}

	//todo check if we are replicating

	rows, err := r.cn.simpleQuery("IDENTIFY_SYSTEM")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make([]driver.Value, 4)

	err = rows.Next(values)
	if err != nil {
		return nil, err
	}

	systemInfo := &IdentifySystemMsg{
		SystemId: values[0].(string),
		Timeline: values[1].(int64),
		XLogPos:  StrToLogPos(values[2].(string)),
		DBName:   values[3].(string),
	}

	return systemInfo, nil
}

func (r *ReplicationConn) CreateLogicalReplicationSlot(name string, outputPlugin string) (*CreateLogicalReplicationSlotResult, error) {
	if r.streaming {
		return nil, errors.New("replication stream already running")
	}

	if r.closed {
		return nil, errors.New("can't run on already closed connection")
	}

	values, err := r.sendReplicationQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s", name, outputPlugin), 4)

	if err != nil {
		return nil, err
	}

	return &CreateLogicalReplicationSlotResult{
		SlotName:     values[0].(string),
		XLogPos:      StrToLogPos(values[1].(string)),
		SnapshotName: values[2].(string),
		OutputPlugin: values[3].(string),
	}, nil
}

// Drops replication slot.
// Returns error if connection is in walsender mode.
func (r *ReplicationConn) DropReplicationSlot(name string) error {
	if r.streaming {
		return errors.New("replication stream already running")
	}

	if r.closed {
		return errors.New("can't run on already closed connection")
	}

	_, err := r.sendReplicationQuery(fmt.Sprintf("DROP_REPLICATION_SLOT %s", name), 0)
	return err
}

func (r *ReplicationConn) sendReplicationQuery(q string, rowsCount int) ([]driver.Value, error) {
	rows, err := r.cn.simpleQuery(q)
	if err != nil {
		return nil, err
	}

	values := make([]driver.Value, rowsCount)

	if rowsCount > 0 {
		err = rows.Next(values)
		if err != nil {
			return nil, err
		}
	}
	rows.Close()

	return values, nil
}

func (r *ReplicationConn) sendQuery(q string) error {
	b := r.cn.writeBuf('Q')
	b.string(q)
	return r.send(b)
}

func (r *ReplicationConn) send(b *writeBuf) (err error) {
	defer errRecoverNoErrBadConn(&err)
	r.cn.send(b)
	return err
}

func (r *ReplicationConn) StartLogicalStream(slotName string, pos LogPos, commitInterval time.Duration, noticeCallback NoticeCallback) error {
	if r.streaming {
		return errors.New("replication stream already running")
	}

	if r.closed {
		return errors.New("can't run replication stream on closed connection")
	}

	r.streaming = true

	r.commitInterval = commitInterval

	q := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s", slotName, pos)

	err := r.sendQuery(q)

	if err != nil {
		return err
	}

	typ := r.cn.recv1Buf(r.rbuf)
	if typ != 'W' {
		return errors.New(fmt.Sprintf("pq: Expected Copy Both mode; got %c, %v", typ, string(*r.rbuf)))
	}

	go r.startAutoCommitLoop()
	go r.startMsgLoop(noticeCallback)

	return nil
}

func (r *ReplicationConn) startMsgLoop(noticeCallback NoticeCallback) {
	var t byte
	var err error
recvMessagesLoop:
	for {
		t, err = r.cn.recvMessage(r.rbuf)

		if err != nil {
			r.err = err
			break recvMessagesLoop
		}

		switch t {
		case 'd':
			typ := r.rbuf.byte()

			if typ == 'k' { //handle keepalive msg
				r.rbuf.next(8)
				mReply := r.rbuf.byte()

				if mReply != 0 {
					err = r.commitLogPos()
					if err != nil {
						r.err = err
						break recvMessagesLoop
					}
				}
			} else if typ == 'w' { //handle copydata
				var offsetChanged bool

				tt := make([]byte, len(*r.rbuf))

				copy(tt, *r.rbuf)

				mStartLogPos := LogPos(r.rbuf.int64())

				r.rbuf.next(16)

				e := &ReplicationEvent{
					LogPos:  mStartLogPos,
					Payload: make([]byte, len(*r.rbuf)),
				}

				copy(e.Payload, r.rbuf.next(len(*r.rbuf)))

				select {
				case r.events <- e:
				case <-r.closeChan:
					break recvMessagesLoop
				}

				r.writeLogPos = mStartLogPos
				if r.commitInterval > -1 && r.flushLogPos != mStartLogPos {
					r.flushLogPos = mStartLogPos
					offsetChanged = true
				}

				if r.commitInterval == 0 && offsetChanged {
					err = r.commitLogPos()
					if err != nil {
						r.err = err
						break recvMessagesLoop
					}
				}
			} else {
				// This should never happen, panic if it does
				errorf("Unknown CopyData message type: %s", string(typ))
			}
		case 'E':
			r.err = parseError(r.rbuf)
			break recvMessagesLoop
		case 'N':
			if noticeCallback != nil {
				noticeCallback(parseNotice(r.rbuf))
			}
		case 'c', 'C': //copy done or command complete
			break recvMessagesLoop
		default:
			r.err = fmt.Errorf("Unknwon response during ReplicationStream %q", t)
			break recvMessagesLoop
		}
	}

	r.closedChan <- struct{}{}

	if r.err != nil {
		r.Close()
	}
}

func (r *ReplicationConn) startAutoCommitLoop() {
	var interval time.Duration
	if r.commitInterval > 0 {
		interval = r.commitInterval
	} else {
		interval = time.Second * 10
	}
	for {
		select {
		case <-time.After(interval):
			err := r.commitLogPos()
			if err != nil {
				r.err = err
				r.Close()
				return
			}
		case <-r.closeChan:
			r.closedChan <- struct{}{}
			return
		}
	}
}

// Events chan will be closed inside Close method
func (r *ReplicationConn) EventsChannel() <-chan *ReplicationEvent {
	return r.events
}

// Commit error will not stop replication stream
func (r *ReplicationConn) MarkFlushLogPos(flushPos LogPos) error {
	if !r.streaming {
		return errors.New("Not in streaming mode")
	} else if r.commitInterval > 0 {
		return errors.New("Can not do this in autocommit mode")
	}
	r.flushLogPos = flushPos
	return r.commitLogPos()
}

func (r *ReplicationConn) Error() error {
	return r.err
}

func (r *ReplicationConn) commitLogPos() error {
	if r.cn.bad || r.closed {
		return fmt.Errorf("Couldn't commit log position on bad or closed connection")
	}

	r.writingLock.Lock()
	defer r.writingLock.Unlock()

	r.wbuf.buf[0] = 'd'
	r.wbuf.buf = r.wbuf.buf[:5]

	r.wbuf.byte('r')
	r.wbuf.int64(int64(r.writeLogPos))
	r.wbuf.int64(int64(r.flushLogPos))
	r.wbuf.int64(0)
	r.wbuf.int64(time.Now().UnixNano()/1000 - 946684800000000) //microseconds since 2000-01-01 00:00:00
	r.wbuf.byte(0)

	return r.send(r.wbuf)
}

func (r *ReplicationConn) Close() (err error) {
	r.closeLock.Lock()
	defer r.closeLock.Unlock()

	if r.closed {
		return r.err
	}
	r.closed = true

	close(r.closeChan)

	if r.streaming {
		r.writingLock.Lock()
		r.wbuf.buf[0] = 'c'
		r.wbuf.buf = r.wbuf.buf[:5]
		err = r.send(r.wbuf)

		r.writingLock.Unlock()

		<-r.closedChan
		<-r.closedChan

		if err == nil {
			err = r.cn.Close()
		}
	} else {
		err = r.cn.Close()
	}

	close(r.events)

	if r.err == nil {
		r.err = err
	}

	return r.err
}

type Notice struct {
	*Error
}

type NoticeCallback func(*Notice)

func parseNotice(r *readBuf) *Notice {
	return &Notice{parseError(r)}
}

//LogPos represents position in PostgreSQL binlog
type LogPos uint64

//Converts position to its textual representation (e.g. 17/A4C41EC0)
func (v LogPos) String() string {
	high := uint32(v >> 32)
	low := uint32(v)
	return fmt.Sprintf("%X/%X", high, low)
}

//Converts textual representation (e.g. 17/A4C41EC0) to LogPos
func StrToLogPos(str string) LogPos {
	var high, low uint32
	fmt.Sscanf(str, "%X/%X", &high, &low)
	return LogPos(int64(high)<<32 | int64(low))
}
