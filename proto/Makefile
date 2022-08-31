raftadminwq.pb.go: raftadminwq.proto
	protoc raftadminwq.proto --go_out=plugins=grpc:. --go_opt=paths=source_relative

force:
	rm -f raftadminwq.pb.go
	make raftadminwq.pb.go
