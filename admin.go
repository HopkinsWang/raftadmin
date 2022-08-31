package raftadminwq

import (
	"context"
	"crypto/sha1"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	//pb "github.com/Jille/raftadminwq/proto"
	//pb "RingAllReduce_29server/raftadminwq/proto"
	pb "github.com/HopkinsWang/raft/raftadmin/proto"
)

type admin struct {
	pb.RaftAdminWqServer
	r *raft.Raft
}

func Get(r *raft.Raft) pb.RaftAdminWqServer {
	return &admin{r:r}
}

func Register(s *grpc.Server, r *raft.Raft) {
	pb.RegisterRaftAdminWqServer(s, Get(r))
}

func timeout(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return dl.Sub(time.Now())
	}
	return 0
}

var (
	mtx        sync.Mutex
	operations = map[string]*future{}
)

type future struct {
	f   raft.Future
	mtx sync.Mutex
}

func toFuture(f raft.Future) (*pb.Future, error) {
	token := fmt.Sprintf("%x", sha1.Sum([]byte(fmt.Sprintf("%d", rand.Uint64()))))
	mtx.Lock()
	operations[token] = &future{f: f}
	mtx.Unlock()
	return &pb.Future{
		OperationToken: token,
	}, nil
}

func (a *admin) Await(ctx context.Context, req *pb.Future) (*pb.AwaitResponse, error) {
	mtx.Lock()
	f, ok := operations[req.GetOperationToken()]
	mtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("token %q unknown", req.GetOperationToken())
	}
	f.mtx.Lock()
	err := f.f.Error()
	f.mtx.Unlock()
	if err != nil {
		return &pb.AwaitResponse{
			Error: err.Error(),
		}, nil
	}
	r := &pb.AwaitResponse{}
	if ifx, ok := f.f.(raft.IndexFuture); ok {
		r.Index = ifx.Index()
	}
	return r, nil
}

func (a *admin) Forget(ctx context.Context, req *pb.Future) (*pb.ForgetResponse, error) {
	mtx.Lock()
	delete(operations, req.GetOperationToken())
	mtx.Unlock()
	return &pb.ForgetResponse{}, nil
}

func (a *admin) AddNonvoter(ctx context.Context, req *pb.AddNonvoterRequest) (*pb.Future, error) {
	return toFuture(a.r.AddNonvoter(raft.ServerID(req.GetId()), raft.ServerAddress(req.GetAddress()), req.GetPreviousIndex(), timeout(ctx)))
}

func (a *admin) AddVoter(ctx context.Context, req *pb.AddVoterRequest) (*pb.Future, error) {
	return toFuture(a.r.AddVoter(raft.ServerID(req.GetId()), raft.ServerAddress(req.GetAddress()), req.GetPreviousIndex(), timeout(ctx)))
}

func (a *admin) AppliedIndex(ctx context.Context, req *pb.AppliedIndexRequest) (*pb.AppliedIndexResponse, error) {
	return &pb.AppliedIndexResponse{
		Index: a.r.AppliedIndex(),
	}, nil
}

func (a *admin) ApplyLog(ctx context.Context, req *pb.ApplyLogRequest) (*pb.Future, error) {
	return toFuture(a.r.ApplyLog(raft.Log{Data: req.GetData(), Extensions: req.GetExtensions()}, timeout(ctx)))
}

func (a *admin) Barrier(ctx context.Context, req *pb.BarrierRequest) (*pb.Future, error) {
	return toFuture(a.r.Barrier(timeout(ctx)))
}

func (a *admin) DemoteVoter(ctx context.Context, req *pb.DemoteVoterRequest) (*pb.Future, error) {
	return toFuture(a.r.DemoteVoter(raft.ServerID(req.GetId()), req.GetPreviousIndex(), timeout(ctx)))
}

func (a *admin) GetConfiguration(ctx context.Context, req *pb.GetConfigurationRequest) (*pb.GetConfigurationResponse, error) {
	f := a.r.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	resp := &pb.GetConfigurationResponse{}
	for _, s := range f.Configuration().Servers {
		cs := &pb.GetConfigurationResponse_Server{
			Id:      string(s.ID),
			Address: string(s.Address),
		}
		switch s.Suffrage {
		case raft.Voter:
			cs.Suffrage = pb.GetConfigurationResponse_Server_VOTER
		case raft.Nonvoter:
			cs.Suffrage = pb.GetConfigurationResponse_Server_NONVOTER
		case raft.Staging:
			cs.Suffrage = pb.GetConfigurationResponse_Server_STAGING
		default:
			return nil, fmt.Errorf("unknown server suffrage %v for server %q", s.Suffrage, s.ID)
		}
		resp.Servers = append(resp.Servers, cs)
	}
	return resp, nil
}

//func (a *admin) LastContact(ctx context.Context, req *pb.LastContactRequest) (*pb.LastContactResponse, error) {
//	t := a.r.LastContact()
//	return &pb.LastContactResponse{
//		UnixNano: t.UnixNano(),
//	}, nil
//}

func (a *admin) LastContact(ctx context.Context, req *pb.LastContactRequest) (*pb.LastContactResponse, error) {

	//timer := time.NewTimer(time.Duration(100))
	//log.Printf("run GetLastContent! ")
	//cnt := 0
	//for {
	//	select {
	//	case <-timer.C:
	//		{
	//			last_idx := a.r.LastIndex()
	//			if last_idx == 0 {
	//				time.Sleep(3)
	//				continue
	//			}
	//
	//			lastlog, getlogerr := a.r.GetLogByIdx(last_idx)
	//			if getlogerr != nil {
	//				log.Printf("GetLogByIdx(last_idx) err: %v", getlogerr)
	//			}
	//			log.Printf("lastindex  = %v",lastlog.Index)
	//			log.Printf("data: %v",string(lastlog.Data))
	//			log.Printf("extension: %v",string(lastlog.Extensions))
	//
	//			if cnt > 60 {
	//				break
	//				//return &pb.LastContentResponse{Data: lastlog.Data, Extensions: lastlog.Extensions, Lastindex: last_idx}, nil
	//			}
	//			timer.Reset(time.Duration(rand.New(rand.NewSource(time.Now().Unix())).Intn(100))) //10 - 1000ms
	//			cnt += 1
	//		}
	//	}
	//}
	//var lastlog raft.Log
	log.Printf(" RUN LastContact!")
	for {
		last_idx := a.r.LastIndex()
		if last_idx > 0 {
			lastlog, getlogerr := a.r.GetLogByIdx(last_idx)
			if getlogerr != nil {
				log.Printf("GetLogByIdx(last_idx) err: %v", getlogerr)
			}
			t := a.r.LastContact()
			return &pb.LastContactResponse{
				UnixNano: t.UnixNano(),
				Index: lastlog.Index,
				Data: lastlog.Data,
				Extension: lastlog.Extensions,
			}, nil
		}
	}
}

func (a *admin) LastIndex(ctx context.Context, req *pb.LastIndexRequest) (*pb.LastIndexResponse, error) {
	return &pb.LastIndexResponse{
		Index: a.r.LastIndex(),
	}, nil
}

func (a *admin) Leader(ctx context.Context, req *pb.LeaderRequest) (*pb.LeaderResponse, error) {
	return &pb.LeaderResponse{
		Address: string(a.r.Leader()),
	}, nil
}

func (a *admin) LeadershipTransfer(ctx context.Context, req *pb.LeadershipTransferRequest) (*pb.Future, error) {
	return toFuture(a.r.LeadershipTransfer())
}

func (a *admin) LeadershipTransferToServer(ctx context.Context, req *pb.LeadershipTransferToServerRequest) (*pb.Future, error) {
	return toFuture(a.r.LeadershipTransferToServer(raft.ServerID(req.GetId()), raft.ServerAddress(req.GetAddress())))
}

func (a *admin) RemoveServer(ctx context.Context, req *pb.RemoveServerRequest) (*pb.Future, error) {
	return toFuture(a.r.RemoveServer(raft.ServerID(req.GetId()), req.GetPreviousIndex(), timeout(ctx)))
}

func (a *admin) Shutdown(ctx context.Context, req *pb.ShutdownRequest) (*pb.Future, error) {
	return toFuture(a.r.Shutdown())
}

func (a *admin) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.Future, error) {
	return toFuture(a.r.Snapshot())
}

func (a *admin) State(ctx context.Context, req *pb.StateRequest) (*pb.StateResponse, error) {
	switch s := a.r.State(); s {
	case raft.Follower:
		return &pb.StateResponse{State: pb.StateResponse_FOLLOWER}, nil
	case raft.Candidate:
		return &pb.StateResponse{State: pb.StateResponse_CANDIDATE}, nil
	case raft.Leader:
		return &pb.StateResponse{State: pb.StateResponse_LEADER}, nil
	case raft.Shutdown:
		return &pb.StateResponse{State: pb.StateResponse_SHUTDOWN}, nil
	default:
		return nil, fmt.Errorf("unknown raft state %v", s)
	}
}

func (a *admin) Stats(ctx context.Context, req *pb.StatsRequest) (*pb.StatsResponse, error) {
	ret := &pb.StatsResponse{}
	ret.Stats = map[string]string{}
	for k, v := range a.r.Stats() {
		ret.Stats[k] = v
	}
	return ret, nil
}

func (a *admin) VerifyLeader(ctx context.Context, req *pb.VerifyLeaderRequest) (*pb.Future, error) {
	return toFuture(a.r.VerifyLeader())
}

func (a *admin) GetLogByIdx(ctx context.Context, req *pb.GetLogByIdxRequest) (*pb.GetLogByIdxResponse, error) {
	//var entry raft.Log
	entry, err := a.r.GetLogByIdx(req.Index)
	if err != nil {
		log.Printf("GetLogByIdx, err = %v", err)
	}
	return &pb.GetLogByIdxResponse{Data: entry.Data, Extensions: entry.Extensions}, nil
}

//func (a *admin) GetLastValidLog(idx uint64)

func (a *admin) GetRaftfutuerByToken(token string) (*future, error) {
	if _, ok := operations[token]; ok {
		return operations[token], nil
	}
	log.Printf("GetRaftfutuerByToken failed,token =%v", token)
	return &future{}, nil
}

// 获取最新内容
func (a *admin) GetLastContent(ctx context.Context, req *pb.LastContentRequest) (*pb.LastContentResponse, error) {
	timer := time.NewTimer(time.Duration(100))
	log.Printf("run GetLastContent! ")
	//var (
	//	lastlog *raft.Log
	//	last_idx uint64
	//)
	cnt := 0
	for {
		select {
		case <-timer.C:
			{
				//lastconresp, lastconerr:= a.r.LastIndex()
				//	//a.LastIndex(context.Background(),&pb.LastIndexRequest{})
				//if lastconerr!=nil{
				//	log.Printf("LastContent err: %v")
				//}
				last_idx := a.r.LastIndex()
				if last_idx == 0 {
					time.Sleep(3)
					continue
				}

				lastlog, getlogerr := a.r.GetLogByIdx(last_idx)
				if getlogerr != nil {
					log.Printf("GetLogByIdx(last_idx) err: %v", getlogerr)
				}
				if cnt > 60 {
					return &pb.LastContentResponse{Data: lastlog.Data, Extensions: lastlog.Extensions, Lastindex: last_idx}, nil
				}
				timer.Reset(time.Duration(rand.New(rand.NewSource(time.Now().Unix())).Intn(100))) //10 - 1000ms
				cnt += 1
			}

		}
	}
	//return  &pb.LastContentResponse{Data:lastlog.Data,Extensions: lastlog.Extensions,Lastindex: last_idx},nil
}
