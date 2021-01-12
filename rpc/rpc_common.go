package rpc

import "errors"

const (
	// cli command
	CliAddLearnerRequest     string = "CliAddLearnerCommand"
	CliAddPeerRequest        string = "CliAddPeerCommand"
	CliChangePeersRequest    string = "CliChangePeersCommand"
	CliGetLeaderRequest      string = "CliGetLeaderCommand"
	CliGetPeersRequest       string = "CliGetPeersCommand"
	CliRemoveLearnersRequest string = "CliRemoveLearnersCommand"
	CliResetLearnersRequest  string = "CliResetLearnersCommand"
	CliResetPeersRequest     string = "CliResetPeersCommand"
	CliSnapshotRequest       string = "CliSnapshotCommand"
	CliTransferLeaderRequest string = "CliTransferLeaderCommand"

	// core command
	CoreAppendEntriesRequest   string = "CoreAppendEntriesCommand"
	CoreGetFileRequest         string = "CoreGetFileCommand"
	CoreInstallSnapshotRequest string = "CoreInstallSnapshotCommand"
	CoreNodeRequest            string = "CoreNodeCommand"
	CoreReadIndexRequest       string = "CoreReadIndexCommand"
	CoreRequestVoteRequest     string = "CoreRequestVoteCommand"
	CoreTimeoutNowRequest      string = "CoreTimeoutNowCommand"
)

var (
	EmptyBytes     []byte = make([]byte, 0)
	ServerNotFount        = errors.New("target server not found")
)
