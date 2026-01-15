# Go Delay Queue

### Consumer Example

```go
c := consumer.NewConsumer(
    []consumer.Cfg{
        {
            Endpoint: "127.0.0.1:11300",
            Tube:     "TEST_TUBE",
        },
        {
            Endpoint: "127.0.0.1:11300",
            Tube:     "TEST_TUBE2",
        },
    },
)

c.OnMessage(func(meta consumer.EventMata, data []byte) {
    log.Printf("endpoint [%s] tube [%s] message [%s].", meta.Endpoint, meta.Tube, string(data))
})

```

### Producer Example
```go
producer := producer.NewProducerNode("127.0.0.1:11300", tube)

for i := 1000; i < 1005; i++ {
    msgId, err := producer.Delay([]byte("消息："+strconv.Itoa(i)), time.Second*5)
    if err != nil {
        fmt.Println(err)
    }
    fmt.Printf("Message: %s\n", msgId)
}
```