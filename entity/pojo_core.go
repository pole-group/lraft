package entity

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/golang/protobuf/proto"

	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

type Status struct {
	state *State
}

type State struct {
	code RaftErrorCode
	msg  string
}

func NewEmptyStatus() Status {
	return Status{state: &State{}}
}

func NewStatus(code RaftErrorCode, msg string) Status {
	return Status{state: &State{
		code: code,
		msg:  msg,
	}}
}

func StatusOK() Status {
	return Status{state: &State{}}
}

func (s Status) Rest() {
	s.state = nil
}

func (s Status) IsOK() bool {
	return s.state == nil || s.state.code == 0
}

func (s Status) SetCode(code RaftErrorCode) {
	if s.state == nil {
		s.state = &State{}
	}
	s.state.code = code
}

func (s Status) GetCode() RaftErrorCode {
	if s.state == nil {
		return 0
	}
	return s.state.code
}

func (s Status) SetMsg(msg string) {
	s.state.msg = msg
}

func (s Status) GetMsg() string {
	if s.state == nil {
		return ""
	}
	return s.state.msg
}

func (s Status) SetError(code RaftErrorCode, format string, args ...interface{}) {
	msg := utils.StringFormat(format, args)
	s.state = &State{
		code: code,
		msg:  msg,
	}
}

func (s Status) Copy() Status {
	return Status{state: &State{
		code: s.state.code,
		msg:  s.state.msg,
	}}
}

type LogId struct {
	index int64
	term  int64
}

func NewEmptyLogID() *LogId {
	return &LogId{
		index: 0,
		term:  0,
	}
}

func NewLogID(index, term int64) *LogId {
	return &LogId{
		index: index,
		term:  term,
	}
}

func (l *LogId) SetIndex(index int64) {
	l.index = index
}

func (l *LogId) SetTerm(term int64) {
	l.term = term
}

func (l *LogId) GetIndex() int64 {
	return l.index
}

func (l *LogId) GetTerm() int64 {
	return l.term
}

func (l *LogId) Compare(other *LogId) int64 {
	c := l.term - other.term
	if c == 0 {
		return l.index - other.index
	}
	return c
}

func (l *LogId) IsEquals(other *LogId) bool {
	if l == other {
		return true
	}
	if other == nil {
		return false
	}
	return l.index == other.index && l.term == other.term
}

func (l *LogId) Checksum() uint64 {
	buf := bytes.NewBuffer([]byte{})
	err := binary.Write(buf, binary.LittleEndian, l.index)
	utils.CheckErr(err)
	err = binary.Write(buf, binary.LittleEndian, l.term)
	utils.CheckErr(err)
	return utils.Checksum(buf.Bytes())
}

// Ballot start

type Ballot struct {
	peers     []*UnFoundPeerId
	quorum    int64
	oldPeers  []*UnFoundPeerId
	oldQuorum int64
}

func (b *Ballot) Init(conf, oldConf *Configuration) bool {
	b.peers = make([]*UnFoundPeerId, 0)
	b.oldPeers = make([]*UnFoundPeerId, 0)
	b.quorum = 0
	b.oldQuorum = 0

	index := int64(0)

	if conf != nil {
		conf.GetPeers().Range(func(value interface{}) {
			b.peers = append(b.peers, &UnFoundPeerId{
				peerId: value.(*PeerId),
				found:  false,
				index:  index,
			})
			index++
		})
	}
	b.quorum = int64(len(b.peers)/2 + 1)
	if oldConf == nil {
		return true
	}
	index = int64(0)
	oldConf.GetPeers().Range(func(value interface{}) {
		b.oldPeers = append(b.oldPeers, &UnFoundPeerId{
			peerId: value.(*PeerId),
			found:  false,
			index:  index,
		})
		index++
	})
	b.oldQuorum = int64(len(b.oldPeers)/2 + 1)
	return true
}

func (b *Ballot) FindPeer(peer *PeerId, peers []*UnFoundPeerId, hint int64) *UnFoundPeerId {
	if hint < 0 || hint > int64(len(b.peers)) || b.peers[hint].peerId.Equal(peer) {
		for _, ufp := range b.peers {
			if ufp.peerId.Equal(peer) {
				return ufp
			}
		}
		return nil
	}
	return b.peers[hint]
}

func (b *Ballot) Grant(peer *PeerId) PosHint {
	return b.GrantWithHint(peer, PosHint{})
}

func (b *Ballot) IsGrant() bool {
	return b.quorum <= 0 && b.oldQuorum <= 0
}

func (b *Ballot) GrantWithHint(peer *PeerId, hint PosHint) PosHint {
	ufp := b.FindPeer(peer, b.peers, hint.PosCurrentPeerId)
	if ufp != nil {
		if !ufp.found {
			ufp.found = true
			b.quorum--
		}
		hint.PosCurrentPeerId = ufp.index
	} else {
		hint.PosCurrentPeerId = -1
	}
	if len(b.oldPeers) == 0 {
		hint.PosOldPeerId = -1
		return hint
	}

	ufp = b.FindPeer(peer, b.oldPeers, hint.PosOldPeerId)
	if ufp != nil {
		if !ufp.found {
			ufp.found = true
			b.oldQuorum--
		}
		hint.PosOldPeerId = ufp.index
	} else {
		hint.PosOldPeerId = -1
	}
	return hint
}

type PosHint struct {
	PosCurrentPeerId int64
	PosOldPeerId     int64
}

type UnFoundPeerId struct {
	peerId *PeerId
	found  bool
	index  int64
}

func NewUnFoundPeerId(peerId *PeerId, found bool, index int64) *UnFoundPeerId {
	return &UnFoundPeerId{
		peerId: peerId,
		found:  found,
		index:  index,
	}
}

func (uf *UnFoundPeerId) GetPeerId() *PeerId {
	return uf.peerId
}

func (uf *UnFoundPeerId) IsFound() bool {
	return uf.found
}

func (uf *UnFoundPeerId) GetIndex() int64 {
	return uf.index
}

// Ballot end

type LeaderChangeContext struct {
	leaderID *PeerId
	term     int64
	status   Status
}

func NewLeaderContext(leaderID *PeerId, term int64, status Status) LeaderChangeContext {
	return LeaderChangeContext{
		leaderID: leaderID,
		term:     term,
		status:   status,
	}
}

func (l LeaderChangeContext) GetLeaderID() *PeerId {
	return l.leaderID
}

func (l LeaderChangeContext) GetTerm() int64 {
	return l.term
}

func (l LeaderChangeContext) GetStatus() Status {
	return l.status
}

func (l LeaderChangeContext) Copy() LeaderChangeContext {
	return LeaderChangeContext{
		leaderID: l.leaderID,
		term:     l.term,
		status:   l.status,
	}
}

type LogEntry struct {
	LogType     raft.EntryType
	LogID       *LogId
	Peers       []*PeerId
	OldPeers    []*PeerId
	Learners    []*PeerId
	OldLearners []*PeerId
	Data        []byte
	checksum    int64
	HasChecksum bool
}

func NewLogEntry(t raft.EntryType) *LogEntry {
	return &LogEntry{
		LogType: t,
	}
}

func (le *LogEntry) HasLearners() bool {
	l := le.Learners != nil && len(le.Learners) != 0
	ol := le.OldLearners != nil && len(le.OldLearners) != 0
	return l || ol
}

func (le *LogEntry) Checksum() uint64 {
	c := utils.Checksum2Long(uint64(le.LogType.Number()), le.LogID.Checksum())
	c = le.checksumPeers(le.Peers, c)
	c = le.checksumPeers(le.OldPeers, c)
	c = le.checksumPeers(le.Learners, c)
	c = le.checksumPeers(le.OldLearners, c)
	if le.Data != nil && len(le.Data) != 0 {
		c = utils.Checksum2Long(c, utils.Checksum(le.Data))
	}
	return c
}

func (le *LogEntry) SetChecksum(checksum int64) {
	le.checksum = checksum
	le.HasChecksum = true
}

func (le *LogEntry) IsCorrupted() bool {
	return le.HasChecksum && le.checksum != int64(le.Checksum())
}

func (le *LogEntry) Encode() []byte {
	pbL := &raft.PBLogEntry{
		Type:        le.LogType,
		Term:        le.LogID.term,
		Index:       le.LogID.index,
		Peers:       le.encodePeers(le.Peers),
		OldPeers:    le.encodePeers(le.OldPeers),
		Data:        le.Data,
		Checksum:    int64(le.Checksum()),
		Learners:    le.encodePeers(le.Learners),
		OldLearners: le.encodePeers(le.OldLearners),
	}
	b, err := proto.Marshal(pbL)
	utils.CheckErr(err)
	return b
}

func (le *LogEntry) Decode(b []byte) {
	pbL := &raft.PBLogEntry{}
	err := proto.Unmarshal(b, pbL)
	utils.CheckErr(err)
}

func (le *LogEntry) encodePeers(peers []*PeerId) [][]byte {
	result := make([][]byte, len(peers))
	for i := 0; i < len(peers); i++ {
		result[i] = peers[i].Encode()
	}
	return result
}

func (le *LogEntry) checksumPeers(peers []*PeerId, c uint64) uint64 {
	if peers != nil && len(peers) != 0 {
		for _, peer := range peers {
			c = utils.Checksum2Long(c, peer.Checksum())
		}
	}
	return c
}

type NodeId struct {
	GroupID string
	Peer    *PeerId
	desc    string
}

func (n *NodeId) GetDesc() string {
	if n.desc == "" {
		n.desc = "<" + n.GroupID + "/" + n.Peer.GetDesc() + ">"
	}
	return n.desc
}

func (n *NodeId) Equal(other *NodeId) bool {
	if n == other {
		return true
	}
	if other == nil {
		return false
	}
	a := strings.Compare(n.GroupID, other.GroupID) == 0
	b := n.Peer != nil && other.Peer != nil
	return a && b && n.Peer.Equal(other.Peer)
}

type Task struct {
}

type UserLog struct {
	Index int64
	Data  []byte
}
