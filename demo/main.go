package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudwego/netpoll"
	"github.com/cloudwego/netpoll/mux"
)

func main() {
	network, address := "tcp", "127.0.0.1:8080"
	listener, _ := netpoll.CreateListener(network, address)

	eventLoop, _ := netpoll.NewEventLoop(
		handle,
		netpoll.WithOnPrepare(prepare),
		netpoll.WithReadTimeout(time.Second),
	)

	// start listen loop ...
	eventLoop.Serve(listener)
}

var _ netpoll.OnPrepare = prepare
var _ netpoll.OnRequest = handle

type connkey struct{}

var ctxkey connkey

func prepare(conn netpoll.Connection) context.Context {
	mc := newSvrMuxConn(conn)
	ctx := context.WithValue(context.Background(), ctxkey, mc)
	return ctx
}

func handle(ctx context.Context, conn netpoll.Connection) (err error) {
	mc := ctx.Value(ctxkey).(*svrMuxConn)
	reader := conn.Reader()

	// bLen, err := reader.Peek(4)
	// if err != nil {
	// 	return err
	// }
	// length := int(binary.BigEndian.Uint32(bLen)) + 4

	// r2, err := reader.Slice(length)
	// if err != nil {
	// 	return err
	// }

	line, err := reader.Until('\n')

	fmt.Println(string(line))
	// handler must use another goroutine
	go func() {
		// req := &codec.Message{}
		// err = codec.Decode(r2, req)
		// if err != nil {
		// 	panic(fmt.Errorf("netpoll decode failed: %s", err.Error()))
		// }

		// // handler
		// resp := req

		// encode
		writer := netpoll.NewLinkBuffer()

		writer.WriteString(string(line))

		//err = codec.Encode(writer, resp)
		if err != nil {
			panic(fmt.Errorf("netpoll encode failed: %s", err.Error()))
		}
		mc.Put(func() (buf netpoll.Writer, isNil bool) {
			return writer, false
		})
	}()
	return nil
}

func newSvrMuxConn(conn netpoll.Connection) *svrMuxConn {
	mc := &svrMuxConn{}
	mc.conn = conn
	mc.wqueue = mux.NewShardQueue(mux.ShardSize, conn)
	return mc
}

type svrMuxConn struct {
	conn   netpoll.Connection
	wqueue *mux.ShardQueue // use for write
}

// Put puts the buffer getter back to the queue.
func (c *svrMuxConn) Put(gt mux.WriterGetter) {
	c.wqueue.Add(gt)
}
