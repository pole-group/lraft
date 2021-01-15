package core

import (
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/mono"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rafterror"
	"github.com/pole-group/lraft/rpc"
	"github.com/pole-group/lraft/transport"
	"github.com/pole-group/lraft/utils"
)

type ReadOnlyOperator struct {
	fsmCaller           FSMCaller
	shutdown            chan int8
	err                 rafterror.RaftError
	pendingNotifyStatus *utils.TreeMap
}

type ReadIndexEvent struct {
	reqCtx       []byte
	done         *ReadIndexClosure
	shutdownWait *sync.WaitGroup
	startTime    time.Time
}

// Topic of the event
func (re *ReadIndexEvent) Name() string {
	return "ReadIndexEvent"
}

// The sequence number of the event
func (re *ReadIndexEvent) Sequence() int64 {
	return time.Now().Unix()
}

type ReadIndexEventSubscriber struct {
	batchEvent []*ReadIndexEvent
	batchSize  int32
	cursor     int32
}

func (res *ReadIndexEventSubscriber) OnEvent(event utils.Event, endOfBatch bool) {
	e := event.(*ReadIndexEvent)
	if e.shutdownWait != nil {
		res.execReadIndexEvent(res.batchEvent)
		res.batchEvent = make([]*ReadIndexEvent, 0, 0)
		e.shutdownWait.Done()
		return
	}
	res.batchEvent[res.cursor] = e
	res.cursor ++
	if res.cursor >= res.batchSize || endOfBatch {
		res.execReadIndexEvent(res.batchEvent)
		res.batchEvent = make([]*ReadIndexEvent, res.batchSize, res.batchSize)
		res.cursor = 0
	}
}

func (res *ReadIndexEventSubscriber) execReadIndexEvent(event []*ReadIndexEvent) {

}

func (res *ReadIndexEventSubscriber) IgnoreExpireEvent() bool {
	return false
}

func (res *ReadIndexEventSubscriber) SubscribeType() utils.Event {
	return &ReadIndexEvent{}
}

// RaftClient 的一些操作
type RaftClientOperator struct {
	rpcClient *rpc.RpcClient
}

func (rcop *RaftClientOperator) PreVote(endpoint entity.Endpoint, req *proto.RequestVoteRequest,
	done *RpcResponseClosure) mono.Mono {

	body, err := ptypes.MarshalAny(req)
	if err != nil {
		return mono.Error(err)
	}

	gRep := &transport.GrpcRequest{
		Label: rpc.CoreRequestVoteRequest,
		Body:  body,
	}

	return invokeWithClosure(endpoint, rcop.rpcClient, gRep, done)
}

func invokeWithClosure(endpoint entity.Endpoint, rpcClient *rpc.RpcClient, req *transport.GrpcRequest,
	done *RpcResponseClosure) mono.Mono {
	future, err := rpcClient.SendRequest(endpoint, req)

	if err != nil {
		future = mono.Error(err)
	}

	return future.DoOnNext(func(v reactor.Any) error {
		resp := v.(*transport.GrpcResponse)
		supplier := rpc.GlobalProtoRegistry.FindProtoMessageSupplier(resp.Label)
		bzResp := supplier()
		if err := ptypes.UnmarshalAny(resp.Body, supplier()); err != nil {
			return err
		}
		done.Resp = bzResp
		done.Run(entity.StatusOK())
		return nil
	}).DoOnCancel(func() {
		done.Run(entity.NewStatus(entity.ECANCELED, "RPC request was canceled by future."))
	}).DoOnError(func(e error) {
		done.Run(entity.NewStatus(entity.UNKNOWN, e.Error()))
	})
}
