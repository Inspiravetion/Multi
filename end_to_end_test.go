package end_to_end

import (
	"fmt"
	. "multi"
	"runtime"
	"sync"
	"testing"
)

func Test_End_To_End(t *testing.T) {

	runtime.GOMAXPROCS(runtime.NumCPU())

	num_messages := 100000

	var count int
	var lock sync.Mutex

	//Easy ways
	New_Stream(10, 10, 10).Produce(5, 2, func(i int, chans *Send_Set) {
		for j := 0; j < num_messages; j++ {
			chans.Send("hello")
		}
	}).Process(5, 2, func(data interface{}, chans *Send_Set) {
		chans.Send(fmt.Sprintf("%s world", data))
	}).Consume_And_Wait(func(data interface{}) {
		msg := data.(string)

		if msg != "hello world" {
			fmt.Println("msg isnt right => ", msg)
			t.Fail()
		}
		lock.Lock()
		count++
		lock.Unlock()
	})

	if count != num_messages {
		fmt.Println("lost some packets => ", count/num_messages, "%")
		t.Fail()
	}
}
