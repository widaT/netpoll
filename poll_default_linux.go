// Copyright 2022 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !race
// +build !race

package netpoll

import (
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Includes defaultPoll/multiPoll/uringPoll...
func openPoll() Poll {
	return openDefaultPoll()
}

func openDefaultPoll() *defaultPoll {
	var poll = defaultPoll{}
	poll.buf = make([]byte, 8)
	var p, err = EpollCreate(0)
	if err != nil {
		panic(err)
	}
	poll.fd = p
	var r0, _, e0 = syscall.Syscall(syscall.SYS_EVENTFD2, 0, 0, 0)
	if e0 != 0 {
		syscall.Close(p)
		panic(err)
	}

	poll.Reset = poll.reset
	poll.Handler = poll.handler

	poll.wop = &FDOperator{FD: int(r0)}
	poll.Control(poll.wop, PollReadable)
	poll.opcache = newOperatorCache()
	return &poll
}

type defaultPoll struct {
	pollArgs
	fd      int            // epoll fd
	wop     *FDOperator    // eventfd, wake epoll_wait
	buf     []byte         // read wfd trigger msg
	trigger uint32         // trigger flag
	opcache *operatorCache // operator cache
	// fns for handle events
	Reset   func(size, caps int)
	Handler func(events []epollevent) (closed bool)
}

type pollArgs struct {
	size     int
	caps     int
	events   []epollevent
	barriers []barrier
	hups     []func(p Poll) error
}

func (a *pollArgs) reset(size, caps int) {
	a.size, a.caps = size, caps
	a.events, a.barriers = make([]epollevent, size), make([]barrier, size)
	for i := range a.barriers {
		a.barriers[i].bs = make([][]byte, a.caps)
		a.barriers[i].ivs = make([]syscall.Iovec, a.caps)
	}
}

// Wait implements Poll.
// 反应堆核心函数
func (p *defaultPoll) Wait() (err error) {
	// init
	var caps, msec, n = barriercap, -1, 0
	p.Reset(128, caps)
	// wait
	for {
		if n == p.size && p.size < 128*1024 { // 控制一次wait接收event的数量
			p.Reset(p.size<<1, caps)
		}
		n, err = EpollWait(p.fd, p.events, msec)
		if err != nil && err != syscall.EINTR {
			return err
		}
		if n <= 0 {
			msec = -1
			runtime.Gosched()
			continue
		}
		msec = 0
		if p.Handler(p.events[:n]) { //处理事件
			return nil
		}
		// we can make sure that there is no op remaining if Handler finished
		p.opcache.free()
	}
}

// 核心函数，处理事件
func (p *defaultPoll) handler(events []epollevent) (closed bool) {
	for i := range events {
		var operator = *(**FDOperator)(unsafe.Pointer(&events[i].data))

		//判断fd （conn） 状态,如果 状态不等1 则继续
		if !operator.do() {
			continue
		}
		// trigger or exit gracefully
		if operator.FD == p.wop.FD {
			// must clean trigger first
			syscall.Read(p.wop.FD, p.buf)
			atomic.StoreUint32(&p.trigger, 0)
			// if closed & exit
			if p.buf[0] > 0 {
				syscall.Close(p.wop.FD)
				syscall.Close(p.fd)
				operator.done()
				return true
			}
			operator.done()
			continue
		}

		evt := events[i].events
		// check poll in
		//输入事件
		if evt&syscall.EPOLLIN != 0 {
			if operator.OnRead != nil { //Listener fd 处理 onread事件
				// for non-connection
				operator.OnRead(p)
			} else if operator.Inputs != nil {
				// for connection
				var bs = operator.Inputs(p.barriers[i].bs) //len(bs) = 1
				if len(bs) > 0 {

					//读取conn中的数据，这边的数据会到connection inputbuffer
					//由于iovec len一直是1 所以这边readv 没办法 批量读取，发挥一次systemcall 获取多次可读数据的功效
					var n, err = readv(operator.FD, bs, p.barriers[i].ivs)

					operator.InputAck(n)

					if err != nil && err != syscall.EAGAIN && err != syscall.EINTR {
						logger.Printf("NETPOLL: readv(fd=%d) failed: %s", operator.FD, err.Error())
						p.appendHup(operator)
						continue
					}
				}
			} else {
				logger.Printf("NETPOLL: operator has critical problem! event=%d operator=%v", evt, operator)
			}
		}

		// check hup
		//对端断开 一般会是 EPOLLIN | EPOLLRDHUP | EPOLLHUP 所以 上面的 Epollin  处理了一轮了
		if evt&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0 {
			p.appendHup(operator)
			continue
		}

		//对应的文件描述符有error
		if evt&syscall.EPOLLERR != 0 {
			// Under block-zerocopy, the kernel may give an error callback, which is not a real error, just an EAGAIN.
			// So here we need to check this error, if it is EAGAIN then do nothing, otherwise still mark as hup.
			if _, _, _, _, err := syscall.Recvmsg(operator.FD, nil, nil, syscall.MSG_ERRQUEUE); err != syscall.EAGAIN {
				p.appendHup(operator)
			} else {
				operator.done()
			}
			continue
		}
		// check poll out
		//输出事件
		if evt&syscall.EPOLLOUT != 0 {
			if operator.OnWrite != nil {
				// for non-connection
				operator.OnWrite(p)
			} else if operator.Outputs != nil {
				// for connection
				var bs, supportZeroCopy = operator.Outputs(p.barriers[i].bs)
				if len(bs) > 0 {
					// TODO: Let the upper layer pass in whether to use ZeroCopy.
					var n, err = sendmsg(operator.FD, bs, p.barriers[i].ivs, false && supportZeroCopy)
					operator.OutputAck(n)
					if err != nil && err != syscall.EAGAIN {
						logger.Printf("NETPOLL: sendmsg(fd=%d) failed: %s", operator.FD, err.Error())
						p.appendHup(operator)
						continue
					}
				}
			} else {
				logger.Printf("NETPOLL: operator has critical problem! event=%d operator=%v", evt, operator)
			}
		}
		operator.done()
	}
	// hup conns together to avoid blocking the poll.
	p.detaches()
	return false
}

// Close will write 10000000
func (p *defaultPoll) Close() error {
	_, err := syscall.Write(p.wop.FD, []byte{1, 0, 0, 0, 0, 0, 0, 0})
	return err
}

// Trigger implements Poll.
func (p *defaultPoll) Trigger() error {
	if atomic.AddUint32(&p.trigger, 1) > 1 {
		return nil
	}
	// MAX(eventfd) = 0xfffffffffffffffe
	_, err := syscall.Write(p.wop.FD, []byte{0, 0, 0, 0, 0, 0, 0, 1})
	return err
}

// Control implements Poll.
func (p *defaultPoll) Control(operator *FDOperator, event PollEvent) error {
	var op int
	var evt epollevent
	*(**FDOperator)(unsafe.Pointer(&evt.data)) = operator
	switch event {
	case PollReadable: // server accept a new connection and wait read
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollWritable: // client create a new connection and wait connect finished
		operator.inuse()
		op, evt.events = syscall.EPOLL_CTL_ADD, EPOLLET|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollModReadable: // client wait read/write
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollDetach: // deregister
		op, evt.events = syscall.EPOLL_CTL_DEL, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollR2RW: // connection wait read/write
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLOUT|syscall.EPOLLRDHUP|syscall.EPOLLERR
	case PollRW2R: // connection wait read
		op, evt.events = syscall.EPOLL_CTL_MOD, syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLERR
	}
	return EpollCtl(p.fd, op, operator.FD, &evt)
}

func (p *defaultPoll) Alloc() (operator *FDOperator) {
	op := p.opcache.alloc()
	op.poll = p
	return op
}

func (p *defaultPoll) Free(operator *FDOperator) {
	p.opcache.freeable(operator)
}

func (p *defaultPoll) appendHup(operator *FDOperator) {
	p.hups = append(p.hups, operator.OnHup)
	if err := operator.Control(PollDetach); err != nil {
		logger.Printf("NETPOLL: poller detach operator failed: %v", err)
	}
	operator.done()
}

func (p *defaultPoll) detaches() {
	if len(p.hups) == 0 {
		return
	}
	hups := p.hups
	p.hups = nil
	go func(onhups []func(p Poll) error) {
		for i := range onhups {
			if onhups[i] != nil {
				onhups[i](p)
			}
		}
	}(hups)
}
