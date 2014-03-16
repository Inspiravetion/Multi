package multi

import (
	"fmt"
	"reflect"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//                               Send_Set                                     //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

type Send_Set struct {
	send_lock sync.Mutex
	cases     []reflect.SelectCase
}

func New_Send_Set(chans []chan interface{}) *Send_Set {
	return new(Send_Set).init(chans)
}

func (this *Send_Set) init(chans []chan interface{}) *Send_Set {
	this.cases = make([]reflect.SelectCase, len(chans))

	for i := 0; i < len(chans); i++ {
		this.cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: reflect.ValueOf(chans[i]),
		}
	}

	return this
}

func (this *Send_Set) Send(val interface{}) {
	this.send_lock.Lock()
	defer this.send_lock.Unlock()

	for i := 0; i < len(this.cases); i++ {
		this.cases[i].Send = reflect.ValueOf(val)
	}

	reflect.Select(this.cases)
}

func (this *Send_Set) Close_All() {
	for i := 0; i < len(this.cases); i++ {
		c, _ := this.cases[i].Chan.Interface().(chan interface{})
		close(c)
	}
}

func (this *Send_Set) Add(c chan interface{}) {
	this.send_lock.Lock()
	defer this.send_lock.Unlock()

	this.cases = append(this.cases, reflect.SelectCase{
		Dir:  reflect.SelectSend,
		Chan: reflect.ValueOf(c),
	})
}

func (this *Send_Set) Channels() []chan interface{} {
	this.send_lock.Lock()
	defer this.send_lock.Unlock()

	chans := make([]chan interface{}, len(this.cases))

	for i := 0; i < len(this.cases); i++ {
		c, _ := this.cases[i].Chan.Interface().(chan interface{})
		chans[i] = c
	}

	return chans
}

////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//                               Recv_Set                                     //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

type Recv_Set struct {
	loop_lock sync.Mutex
	case_lock sync.Mutex
	cases     []reflect.SelectCase
}

func New_Recv_Set(chans []chan interface{}) *Recv_Set {
	return new(Recv_Set).init(chans)
}

func (this *Recv_Set) init(chans []chan interface{}) *Recv_Set {
	this.cases = make([]reflect.SelectCase, len(chans))

	for i := 0; i < len(chans); i++ {
		this.cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(chans[i]),
		}
	}

	return this
}

func (this *Recv_Set) is_empty() bool {
	this.case_lock.Lock()
	defer this.case_lock.Unlock()

	empty := len(this.cases) <= 0
	return empty
}

func (this *Recv_Set) Add(c chan interface{}) {
	this.case_lock.Lock()
	defer this.case_lock.Unlock()

	this.cases = append(this.cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(c),
	})
}

func (this *Recv_Set) Next() (interface{}, bool) {
	this.loop_lock.Lock()

	if !this.is_empty() {

		chosen_index, value, still_open := reflect.Select(this.cases)

		if !still_open {
			this.case_lock.Lock()
			this.cases[chosen_index] = this.cases[len(this.cases)-1]
			this.cases = this.cases[0 : len(this.cases)-1]
			this.case_lock.Unlock()
			this.loop_lock.Unlock()
			return this.Next()
		}

		this.loop_lock.Unlock()
		return value.Interface(), false

	} else {
		this.loop_lock.Unlock()
		return nil, true
	}

}

func (this *Recv_Set) Channels() []chan interface{} {
	this.case_lock.Lock()
	defer this.case_lock.Unlock()

	chans := make([]chan interface{}, len(this.cases))

	for i := 0; i < len(this.cases); i++ {
		c, _ := this.cases[i].Chan.Interface().(chan interface{})
		chans[i] = c
	}

	return chans

}

////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//                               PRODUCER_POOL                                //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

type Producer func(int, *Send_Set)

type Producer_Pool struct {
	wg        sync.WaitGroup
	chans     []chan interface{}
	send_set  *Send_Set
	num_prods int
	num_chans int
	buff_size int
}

func New_Producer_Pool(num_prods, num_chans, buff_size int) *Producer_Pool {
	return new(Producer_Pool).init(num_prods, num_chans, buff_size)
}

func (this *Producer_Pool) init(num_prods, num_chans, buff_size int) *Producer_Pool {

	this.buff_size = buff_size
	this.num_prods = num_prods
	this.num_chans = num_chans

	this.send_set = New_Send_Set(make([]chan interface{}, 0))

	return this
}

func (this *Producer_Pool) Start(producer Producer) []chan interface{} {
	chans := make([]chan interface{}, this.num_chans)

	for i := 0; i < this.num_chans; i++ {
		if this.buff_size > 1 {
			chans[i] = make(chan interface{}, this.buff_size)
			this.send_set.Add(chans[i])
		} else {
			chans[i] = make(chan interface{})
			this.send_set.Add(chans[i])
		}
	}

	this.wg.Add(this.num_prods)

	for i := 0; i < this.num_prods; i++ {
		go func(i int) {
			defer this.wg.Done()
			producer(i, this.send_set)
		}(i)
	}

	go func() {
		this.wg.Wait()
		this.send_set.Close_All()
	}()

	return chans
}

////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//                               CONSUMER_POOL                                //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

type Consumer func(interface{})

type Consumer_Pool struct {
	num_cons int
	wg       sync.WaitGroup
}

func New_Consumer_Pool(num_cons int) *Consumer_Pool {
	return new(Consumer_Pool).init(num_cons)
}

func (this *Consumer_Pool) init(num_cons int) *Consumer_Pool {
	this.num_cons = num_cons

	return this
}

func (this *Consumer_Pool) Start(chans []chan interface{}, consumer Consumer) {
	recv_set := New_Recv_Set(chans)

	this.wg.Add(this.num_cons)

	for i := 0; i < this.num_cons; i++ {
		go func() {
			defer this.wg.Done()
			for data, done := recv_set.Next(); !done; data, done = recv_set.Next() {
				consumer(data)
			}
		}()
	}

}

func (this *Consumer_Pool) Wait() {
	this.wg.Wait()
}

////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//                               MIDDLEWARE_POOL                              //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

type Middleware func(interface{}, *Send_Set)

type Middleware_Pool struct {
	wg        sync.WaitGroup
	send_set  *Send_Set
	num_chans int
	num_heads int
	buff_size int
}

func New_Middleware_Pool(num_heads, num_chans, buf_size int) *Middleware_Pool {
	return new(Middleware_Pool).init(num_heads, num_chans, buf_size)
}

func (this *Middleware_Pool) init(num_heads, num_chans, buf_size int) *Middleware_Pool {
	this.num_chans = num_chans
	this.num_heads = num_heads
	this.buff_size = buf_size

	this.send_set = New_Send_Set(make([]chan interface{}, 0))

	return this
}

func (this *Middleware_Pool) Start(in_chans []chan interface{}, middleware Middleware) []chan interface{} {
	chans := make([]chan interface{}, this.num_chans)

	for i := 0; i < this.num_chans; i++ {
		if this.buff_size > 1 {
			chans[i] = make(chan interface{}, this.buff_size)
			this.send_set.Add(chans[i])
		} else {
			chans[i] = make(chan interface{})
			this.send_set.Add(chans[i])
		}
	}

	this.wg.Add(this.num_heads)

	recv_set := New_Recv_Set(in_chans)

	for i := 0; i < this.num_heads; i++ {
		go func() {
			defer this.wg.Done()
			for data, done := recv_set.Next(); !done; data, done = recv_set.Next() {
				middleware(data, this.send_set)
			}
		}()
	}

	go func() {
		this.wg.Wait()
		this.send_set.Close_All()
	}()

	return chans

}

////////////////////////////////////////////////////////////////////////////////
//                                                                            //
//                               Stream                                       //
//                                                                            //
////////////////////////////////////////////////////////////////////////////////

type Stream struct {
	prod_chans []chan interface{}
	num_prod   int
	num_midd   int
	num_cons   int
}

func New_Stream(prods, midds, cons int) *Stream {
	return new(Stream).init(prods, midds, cons)
}

func (this *Stream) init(prods, midds, cons int) *Stream {
	this.num_prod = prods
	this.num_midd = midds
	this.num_cons = cons

	return this
}

func (this *Stream) ensure_producer() {
	if this.prod_chans == nil {
		fmt.Println("Error: Streams must Start with a producer")
	}
}

func (this *Stream) Produce(num_out_chans, buff_size int, producer Producer) *Stream {
	this.prod_chans = New_Producer_Pool(this.num_prod, num_out_chans, buff_size).Start(producer)

	fmt.Println("PRODUCER: ", len(this.prod_chans), " chans")

	return this
}

func (this *Stream) Process(num_out_chans, buff_size int, middleware Middleware) *Stream {
	this.ensure_producer()

	fmt.Println("PROCESS Before: ", len(this.prod_chans), " chans")

	this.prod_chans = New_Middleware_Pool(this.num_midd, num_out_chans, buff_size).Start(this.prod_chans, middleware)

	fmt.Println("PROCESS After: ", len(this.prod_chans), " chans")

	return this
}

func (this *Stream) Consume(consumer Consumer) {
	this.ensure_producer()

	fmt.Println("CONSUMER: ", len(this.prod_chans), " chans")

	New_Consumer_Pool(this.num_cons).Start(this.prod_chans, consumer)
}

func (this *Stream) Consume_And_Wait(consumer Consumer) {
	this.ensure_producer()

	cons_pool := New_Consumer_Pool(this.num_cons)
	cons_pool.Start(this.prod_chans, consumer)
	cons_pool.Wait()
}

func (this *Stream) Collect(num_out_chans, buff_size int, middleware Middleware) []chan interface{} {
	this.ensure_producer()

	return New_Middleware_Pool(this.num_midd, num_out_chans, buff_size).Start(this.prod_chans, middleware)
}
