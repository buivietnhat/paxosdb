test: test_paxos test_kvpaxos

test_paxos:
	cd pkg/paxos && go test -cover

test_kvpaxos:
	cd pkg/kvpaxos && go test -cover
