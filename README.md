Multi
=====

Concurrency helpers for dealing with arbitrary numbers of channels and goroutines. 

```go
  //Start a new Multi Stream with 30 goroutines; 10 producers, 10 processors, and 10 consumers
  //Have the producer goroutines multiplex their sends across 5 channels with a buffer size of two
  New_Stream(10, 10, 10).Produce(5, 2, func(prod_idx int, chans *Send_Set) {
  
    //Each producer sends "Hello" 5 times
    for i := 0; i < 5; i++ {
      chans.Send("Hello")
    }
  
  //Have the processer goroutines multiplex their sends across 5 channels with a buffer size of two
  }).Process(5, 2, func(data interface{}, chans *Send_Set) {
  
    //Each processer sends "Hello World!"
    chans.send(fmt.Sprintf("%s World!", data)
  
  //Start consumers and block this goroutine until they finish
  }).Consume_And_Wait(func(data interface{}) {
  
    //Print "Hello World!"
    msg := data.(string)
    fmt.Println(msg)
  
  })
  
  //"Hello World!" has been printed 50x by this point
  fmt.Println("All Producer, Processer, and Consumer goroutines done.")
```
