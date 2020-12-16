package rc_go

import (
	"fmt"
	"log"
	"os"
	"time"
	"bufio"
	"io"
	"encoding/binary"
	"sync"

	"github.com/Cjen1/rc_go/OpWire"
	"github.com/golang/protobuf/proto"
)

func unix_seconds(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

type Client interface {
	Put(string, string) error
	Get(string) (string, error)
	Close ()
}

func put(res_ch chan *OpWire.Response, cli Client, op *OpWire.Request_Operation_Put, clientid uint32, expected_start float64) {
	st := unix_seconds(time.Now())
	var err error
	if true {
		err = cli.Put(string(op.Put.Key), string(op.Put.Value))
	}
	end := unix_seconds(time.Now())

	err_msg := "None"
	if err != nil {
		err = cli.Put(string(op.Put.Key), string(op.Put.Value))
		if err != nil {
			err_msg = "ERROR WAS FOUND: " + err.Error()
		}
	}

	resp := &OpWire.Response{
		ResponseTime: end - expected_start,
		Err:          err_msg,
		ClientStart:  st,
		QueueStart:   expected_start,
		End:          end,
		Clientid:     clientid,
		Optype:       "Write",
	}

	res_ch <- resp
}

func get(res_ch chan *OpWire.Response, cli Client, op *OpWire.Request_Operation_Get, clientid uint32, expected_start float64) {
	st := unix_seconds(time.Now())
	_, err := cli.Get(string(op.Get.Key))
	end := unix_seconds(time.Now())

	err_msg := "None"
	if err != nil {
		err_msg = "ERROR WAS FOUND: " + err.Error()
	}

	resp := &OpWire.Response{
		ResponseTime: end - expected_start,
		Err:          err_msg,
		ClientStart:  st,
		QueueStart:   expected_start,
		End:          end,
		Clientid:     clientid,
		Optype:       "Read",
	}

	res_ch <- resp
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func perform(op OpWire.Request_Operation, res_ch chan *OpWire.Response, cli Client, clientid uint32, start_time float64) {
	expected_start := op.Start + start_time
	switch op_t := op.OpType.(type) {
	case *OpWire.Request_Operation_Put:
		put(res_ch, cli, op_t, clientid, expected_start)
	case *OpWire.Request_Operation_Get:
		get(res_ch, cli, op_t, clientid, expected_start)
	default:
		resp := &OpWire.Response{
			ResponseTime: -1,
			Err:          fmt.Sprintf("Error: Operation (%v) was not found / supported", op),
			Clientid:     clientid,
			Optype:       "Error",
		}
		res_ch <- resp
	}
}

func recv(reader *bufio.Reader) *OpWire.Request{
	var size int32
	if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
		log.Fatal(err)
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		log.Fatal(err)
	}

	op := &OpWire.Request{}
	if err := proto.Unmarshal([]byte(payload), op); err != nil {
		log.Fatal("Failed to parse incomming operation")
	}
	log.Printf("received op size = %d\n", size)
	return op
}

func marshall_response(resp *OpWire.Response) []byte {
	payload, err := proto.Marshal(resp)
	check(err)
	return payload
}

func send(writer *os.File, msg []byte){
	var size int32
	size = int32(len(msg))
	log.Printf("send size = %d", size)
	size_part := make([]byte, 4)
	// uint32 doesn't change sign bit, just how value is interpreted
	binary.LittleEndian.PutUint32(size_part, uint32(size))

	payload := append(size_part[:], msg[:]...)
	_, err := writer.Write(payload)
	check(err)
}

func init() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

func result_loop(res_ch chan *OpWire.Response, out *os.File, done chan bool) {
	var results []*OpWire.Response
	log.Print("Starting result loop")
	for res := range res_ch {
		results = append(results, res)
	}

	for _, res := range results {
		payload := marshall_response(res)
		log.Print("Sending response")
		send(out, payload)
	}
	done <- true
}

func consume_ignore(res_ch chan *OpWire.Response) {
	for range res_ch {}
}

func Run(client_gen func() (Client, error), clientid uint32, result_pipe string, drop_overloaded bool, new_client_per_request bool) {
	log.Print("Client: Starting run")
	log.Printf("Client: creating file")
	log.Print(result_pipe)
	file, err := os.OpenFile(result_pipe, os.O_WRONLY, 0755)
	check(err)
	defer file.Close()
	out_writer := file

	cli, err := client_gen()
	defer cli.Close()
	check(err)

	reader := bufio.NewReader(os.Stdin)

	//Phase 1: preload
	log.Print("Phase 1: preload")
	var ops []*OpWire.Request_Operation
	got_finalise := false
	bh_ch := make(chan *OpWire.Response)
	go consume_ignore(bh_ch)
	for !got_finalise {
		op := recv(reader)
		log.Print(op)

		switch op.Kind.(type) {
		case *OpWire.Request_Op:
			if op.GetOp().Prereq {
				log.Print("Performing prereq")
				perform(*op.GetOp(), bh_ch, cli, clientid, unix_seconds(time.Now()))
			} else {
				ops = append(ops, op.GetOp())
			}
		case *OpWire.Request_Finalise_:
			got_finalise = true
		default:
			log.Print("Got unrecognised message...")
			log.Print(op)
			log.Fatal("Quitting due to unrecognised message")
		}
	}
	close(bh_ch)

	//Phase 2: Readying
	log.Print("Phase 2: Readying")

	//Create client channels
	nchannels := 1000
	conns := make([]chan OpWire.Request_Operation, nchannels)
	for i := 0; i < nchannels; i++ {
		conns[i] = make(chan OpWire.Request_Operation, 10)
	}

	//Dispatch results loop
	res_ch := make(chan *OpWire.Response, 50000)
	done := make(chan bool)
	go result_loop(res_ch, out_writer, done)

	start_time := new(time.Time)
	wait_bar := make(chan struct{})
	var wg_perform sync.WaitGroup
	closeClients := func () {}
	if !new_client_per_request {
		//Create clients
		nclients := 100
		clients := make([]Client, nclients)
		for i := 0; i < nclients; i++ {
			cli, err := client_gen()
			check(err)
			clients[i] = cli
		}

		//Dispatch concurrent clients
		for i, ch := range conns {
			wg_perform.Add(1)
			go func(cli Client, ch chan OpWire.Request_Operation, t *time.Time) {
				defer wg_perform.Done()
				<-wait_bar
				start_time := *t
				for op := range ch {
					perform(op, res_ch, cli, clientid, unix_seconds(start_time))
				}
			} (clients[i%nclients], ch, start_time)
		}
		closeClients = func () {
			for _, cli := range clients {
				cli.Close()
			}
		}
	} else {
		//Dispatch concurrent clients
		for _, ch := range conns {
			wg_perform.Add(1)
			go func(ch chan OpWire.Request_Operation, t *time.Time) {
				defer wg_perform.Done()
				<-wait_bar
				start_time := *t
				for op := range ch {
					cli, err := client_gen()
					check(err)
					perform(op, res_ch, cli, clientid, unix_seconds(start_time))
					cli.Close()
				}
			} (ch, start_time)
		}
	}


	//signal ready
	send(out_writer, []byte(""))

	//Phase 3: Execute
	log.Print("Phase 3: Execute")
	got_start := false
	for !got_start{
		op := recv(reader)
		switch op.Kind.(type) {
		case *OpWire.Request_Start_:
			log.Print("Got start_request")
			got_start = true
		default:
			log.Fatal("Got a message which wasn't a start!")
		}
	}

	log.Print("Starting to perform ops")
	*start_time = time.Now()
	close(wait_bar)
	for i, op := range ops {
		end_time := start_time.Add(time.Duration(op.Start * float64(time.Second)))
		t := time.Now()
		for ; t.Before(end_time); t = time.Now() {}
		if drop_overloaded {
			select {
			case conns[i % nchannels] <- *op:
			default:
				log.Printf("Skipping op, channel is blocked")
			}
		} else {
			conns[i % nchannels] <- *op
		}
	}
	log.Print("Finished sending ops")

	for _, ch := range conns {
		close(ch)
	}

	//Wait to complete ops
	wg_perform.Wait()

	//Signal end of results 
	close(res_ch)
	//Wait for results to be returned to generator
	<-done

	closeClients()
}
