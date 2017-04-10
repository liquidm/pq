package main

import (
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/liquidm/pq"
	"github.com/liquidm/pq/decoderbufs"
)

func main() {
	slot := "pqgotest_slot"

	conn, err := pq.NewReplicationConnection("host=localhost dbname=pqgotest user=postgres sslmode=disable")
	if err != nil {
		panic(err)
	}

	id, err := conn.IdentifySystem()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Identifying system\n - SystemId: %s\n - Timeline: %d\n - XLogPos: %s\n - DBName: %s\n", id.SystemId, id.Timeline, id.XLogPos, id.DBName)

	switch mode := os.Getenv("MODE"); mode {

	case "reset":
		fmt.Printf("Dropping replication slot %s\n", slot)

		conn.DropReplicationSlot(slot)

		fallthrough

	case "create":
		fmt.Printf("Creating replication slot %s\n", slot)

		_, err := conn.CreateLogicalReplicationSlot(slot, "decoderbufs")
		if err != nil {
			panic(err)
		}

		fallthrough

	default:
		fmt.Printf("Starting streaming %s from 0/0\n", slot)

		conn.StartLogicalStream(slot, "0/0", 5*time.Second, func(n *pq.Notice) { fmt.Printf("Got notice: %s\n", n.Error) })
	}

	for {
		select {
		case e := <-conn.EventsChannel():
			payload := &decoderbufs.RowMessage{}

			// The first int64 of message payload is the length of the following protobuf content.
			// See: https://github.com/liquidm/decoderbufs/blob/master/src/decoderbufs.c#L534-L537
			err := proto.Unmarshal(e.Payload[8:], payload)
			if err != nil {
				panic(err)
			}

			fmt.Printf("Got event: %d, %s\n", pq.XLogPosIntToStr(e.LogPos), payload)
		}
	}

}
