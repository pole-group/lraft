# pole-rpc

![Go Report Card](https://goreportcard.com/badge/github.com/pole-group/pole-rpc)

Very simple RPC remote calling framework, using declarative calling strategy

非常简单的RPC远程调用框架，采用声明式的调用策略

## TODO

- [x] 监听TCP链接的`Connect`以及`DisConnect`事件
- [x] `net.Conn`的管理
- [x] `RSocket`的`RPC`调用实现
- [x] 使用`Option`设计模式初始化各个组件
- [x] `logger`、基本数据结构、`scheduler`调度实现
- [ ] 更易于使用的体验
- [ ] `metrics`指标设置
- [ ] 基于`websocket`实现`RPC`调用
- [ ] 基于`http2`实现`RPC`调用

## Example

```go
func main() {
    ctx, cancelF := context.WithCancel(context.Background())
    defer cancelF()
    
    TestPort = 8000 + rand.Int31n(1000)
    server := createTestServer(ctx)
    // 等待 Server 的启动
    <-server.IsReady
    
    if err := <-server.ErrChan; err != nil {
        t.Error(err)
        return
    }
    
    client, err := createTestClient()
    if err != nil {
        t.Error(err)
        return
    }
    server.RegisterChannelRequestHandler(TestHelloWorld, func (cxt context.Context, rpcCtx RpcServerContext) {
        fmt.Printf("receive client requst : %#v\n", rpcCtx.GetReq())
        for i := 0; i < 10; i++ {
            _ = rpcCtx.Send(&ServerResponse{
                Code: int32(i),
            })
        }
    })
    waitG := sync.WaitGroup{}
    waitG.Add(10)
    
    uuidHolder := atomic.Value{}
    
    call := func (resp *ServerResponse, err error) {
        waitG.Done()
        RpcLog.Info("response %#v", resp)
        assert.Equalf(t, uuidHolder.Load().(string), resp.RequestId, "req-id must equal")
    }
    
    rpcCtx, err := client.RequestChannel(ctx, createTestEndpoint(), call)
    if err != nil {
        t.Error(err)
        return
    }
    
    reqId := uuid.New().String()
    uuidHolder.Store(reqId)
    rpcCtx.Send(&ServerRequest{
        FunName:   TestHelloWorld,
        RequestId: reqId,
    })
    waitG.Wait()
    
    waitG.Add(10)
    reqId = uuid.New().String()
    uuidHolder.Store(reqId)
    rpcCtx.Send(&ServerRequest{
        FunName:   TestHelloWorld,
        RequestId: reqId,
    })
    waitG.Wait()
}
```