package multi

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func Recv_Set_Test(t *testing.T, buff_sz int) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//Start mock producers
	var wg sync.WaitGroup
	num_chans := 10
	chans := make([]chan interface{}, num_chans)
	wg.Add(num_chans)

	for i := 0; i < num_chans; i++ {
		if buff_sz > 1 {
			chans[i] = make(chan interface{}, buff_sz)
		} else {
			chans[i] = make(chan interface{})
		}

		go func(i int) {
			c := chans[i]
			for j := 0; j < num_chans; j++ {
				c <- 1
			}
			wg.Done()
		}(i)
	}

	//make sure they close
	go func() {
		wg.Wait()
		for i := 0; i < len(chans); i++ {
			close(chans[i])
		}
	}()

	var wg2 sync.WaitGroup
	recv_set := New_Recv_Set(chans)
	results := make(chan interface{})
	num_cons := num_chans / 2
	wg2.Add(num_cons)

	for i := 0; i < num_cons; i++ {
		go func() {
			count := 0

			for data, done := recv_set.Next(); !done; data, done = recv_set.Next() {
				inc := data.(int)
				count += inc
			}

			results <- count
			wg2.Done()
		}()
	}

	go func() {
		wg2.Wait()
		close(results)
	}()

	final_count := 0
	for res := range results {
		num := res.(int)
		final_count += num
	}

	if final_count != (num_chans * num_chans) {
		fmt.Println("expected: ", num_chans*num_chans, " ... got: ", final_count)
		t.Fail()
	}
}

func Send_Set_Test(t *testing.T, buff_sz int) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	//Start mock recievers
	var results_wg sync.WaitGroup
	num_chans := 10
	chans := make([]chan interface{}, num_chans)
	results := make(chan interface{})
	results_wg.Add(num_chans)

	for i := 0; i < num_chans; i++ {
		if buff_sz > 1 {
			chans[i] = make(chan interface{}, buff_sz)
		} else {
			chans[i] = make(chan interface{})
		}

		go func(i int) {
			c := chans[i]
			for val := range c {
				results <- val
			}
			results_wg.Done()
		}(i)
	}

	//make sure to close results when all the feeder chans are done
	go func() {
		results_wg.Wait()
		close(results)
	}()

	//setup producers
	var prod_wg sync.WaitGroup
	prod_wg.Add(num_chans)
	send_set := New_Send_Set(chans)

	for i := 0; i < num_chans; i++ {
		go func() {
			for j := 0; j < num_chans; j++ {
				send_set.Send(1)
			}
			prod_wg.Done()
		}()
	}

	go func() {
		prod_wg.Wait()
		send_set.Close_All()
	}()

	final_count := 0
	for res := range results {
		num := res.(int)
		final_count += num
	}

	if final_count != (num_chans * num_chans) {
		fmt.Println("expected: ", num_chans*num_chans, " ... got: ", final_count)
		t.Fail()
	}

}

func Test_Recv_Set_Unbuffered(t *testing.T) {
	Recv_Set_Test(t, -1)
}

func Test_Recv_Set_Buffered(t *testing.T) {
	Recv_Set_Test(t, 5)
}

func Test_Send_Set_Unbuffered(t *testing.T) {
	Send_Set_Test(t, -1)
}

func Test_Send_Set_Buffered(t *testing.T) {
	Send_Set_Test(t, 5)
}

// func Test_End_To_End(t *testing.T) {

// 	runtime.GOMAXPROCS(runtime.NumCPU())

// 	num_messages := 100000

// 	var count int
// 	var lock sync.Mutex

// 	//Easy ways
// 	New_Stream(10, 10, 10).Produce(5, 2, func(i int, chans *Send_Set) {
// 		for j := 0; j < num_messages; j++ {
// 			chans.Send("hello")
// 		}
// 	}).Process(5, 2, func(data interface{}, chans *Send_Set) {
// 		chans.Send(fmt.Sprintf("%s world", data))
// 	}).Consume_And_Wait(func(data interface{}) {
// 		msg := data.(string)

// 		if msg != "hello world" {
// 			fmt.Println("msg isnt right => ", msg)
// 			t.Fail()
// 		}
// 		lock.Lock()
// 		count++
// 		lock.Unlock()
// 	})

// 	if count != num_messages {
// 		fmt.Println("lost some packets => ", count/num_messages, "%")
// 		t.Fail()
// 	}
// }
