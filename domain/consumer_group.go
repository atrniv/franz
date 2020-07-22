package domain

import (
	"sync"
	"time"

	"github.com/atrniv/franz/protocol"
	"github.com/atrniv/franz/util"
	"github.com/rs/zerolog/log"
)

type ConsumerGroupState string

const (
	ConsumerGroupStateDown       ConsumerGroupState = "DOWN"
	ConsumerGroupStateInitialize ConsumerGroupState = "INITIALIZE"
	ConsumerGroupStateStable     ConsumerGroupState = "STABLE"
	ConsumerGroupStateJoining    ConsumerGroupState = "JOINING"
	ConsumerGroupStateAwaitSync  ConsumerGroupState = "AWAIT-SYNC"
	ConsumerGroupStateSyncing    ConsumerGroupState = "SYNCING"
)

type ConsumerGroup struct {
	ID                        string
	generation                int32
	leader                    string
	assignmentProtocol        string
	expectedMembers           int
	currentMembers            int
	assignments               []protocol.SyncGroupAssignment
	state                     ConsumerGroupState
	stateMutex                sync.Mutex
	members                   map[string]*ConsumerGroupMember
	offsets                   map[string]map[int32]CommittedOffset
	initialRebalanceTimeoutMs int32
	joinTimer                 *time.Timer
	sync.RWMutex
}

type CommittedOffset struct {
	Offset      int64
	LeaderEpoch int32
	Timestamp   int64
	Metadata    string
}

func (g *ConsumerGroup) Reset() {
	g.Lock()
	switch g.state {
	case ConsumerGroupStateJoining:
		if g.joinTimer != nil && g.joinTimer.Stop() {
			g.generation++
			g.stateMutex.Unlock()
		}
		g.assignmentProtocol = ""
		g.state = ConsumerGroupStateStable
		g.leader = ""
		g.joinTimer = nil
		g.members = map[string]*ConsumerGroupMember{}
		g.currentMembers = 0
		g.assignments = []protocol.SyncGroupAssignment{}
	case ConsumerGroupStateAwaitSync:
		g.assignmentProtocol = ""
		g.state = ConsumerGroupStateStable
		g.leader = ""
		g.joinTimer = nil
		g.members = map[string]*ConsumerGroupMember{}
		g.currentMembers = 0
		g.assignments = []protocol.SyncGroupAssignment{}
	case ConsumerGroupStateSyncing:
		g.stateMutex.Unlock()
		g.assignmentProtocol = ""
		g.state = ConsumerGroupStateStable
		g.leader = ""
		g.joinTimer = nil
		g.members = map[string]*ConsumerGroupMember{}
		g.currentMembers = 0
		g.assignments = []protocol.SyncGroupAssignment{}
	default:
		log.Trace().Msg("Nothing to do for the consumer group")
	}
	g.Unlock()
}

func (g *ConsumerGroup) IsReady() bool {
	g.RLock()
	defer g.RUnlock()
	return g.currentMembers >= g.expectedMembers
}

func (g *ConsumerGroup) Generation() int32 {
	g.RLock()
	defer g.RUnlock()
	return g.generation
}

func (g *ConsumerGroup) Leader() string {
	g.RLock()
	defer g.RUnlock()
	return g.leader
}

func (g *ConsumerGroup) AssignmentProtocol() string {
	g.RLock()
	defer g.RUnlock()
	return g.assignmentProtocol
}

func (g *ConsumerGroup) Members() map[string]*ConsumerGroupMember {
	g.RLock()
	defer g.RUnlock()
	return g.members
}

func (g *ConsumerGroup) CommittedOffset(topic string, partition int32) int64 {
	g.RLock()
	defer g.RUnlock()
	topicPartitions, exists := g.offsets[topic]
	if !exists {
		return -1
	}
	offset, exists := topicPartitions[partition]
	if !exists {
		return -1
	}

	return offset.Offset - 1
}

func (g *ConsumerGroup) CommitOffset(topic string, partition int32, offset int64, leaderEpoch int32, timestamp int64, metadata string) {
	g.Lock()
	topicPartitions, exists := g.offsets[topic]
	if !exists {
		topicPartitions = map[int32]CommittedOffset{}
	}
	topicPartitions[partition] = CommittedOffset{
		Offset:      offset,
		LeaderEpoch: leaderEpoch,
		Timestamp:   timestamp,
		Metadata:    metadata,
	}
	g.offsets[topic] = topicPartitions
	g.Unlock()
}

func (g *ConsumerGroup) findCompatibleAssignmentProtocol(timestamp time.Time) {
	protocols := []string{}

	for _, member := range g.members {
		if member.GenerationID != g.generation {
			continue
		}
		if len(protocols) == 0 && len(member.Metadata) > 0 {
			for _, protocol := range member.Metadata {
				protocols = append(protocols, protocol.Name)
			}
			break
		}
	}

	for _, protocol := range protocols {
		allMembersCompatible := true
		for _, member := range g.members {
			if member.GenerationID != g.generation {
				continue
			}
			compatible := false
			for _, supportedProtocol := range member.Metadata {
				if supportedProtocol.Name == protocol {
					compatible = true
					break
				}
			}
			if !compatible {
				allMembersCompatible = false
			}
		}
		if allMembersCompatible == true {
			log.Trace().Str("group_id", g.ID).Int32("generation_id", g.generation).Str("protocol", protocol).Msg("Assignment protocol finalized")
			g.assignmentProtocol = protocol
			break
		}
	}
}

func (g *ConsumerGroup) Heartbeat(memberID string, generationID int32, timestamp time.Time) error {
	g.Lock()
	defer g.Unlock()
	if g.state == ConsumerGroupStateDown || g.state == ConsumerGroupStateInitialize {
		return ErrCoordinatorNotAvailable
	}
	// NOTE: Hearbeats can be accepted in AwaitingSync and Syncing states
	if g.state == ConsumerGroupStateJoining {
		return ErrRebalanceInProgress
	}
	found := false
	for _, member := range g.members {
		if member.ID == memberID {
			if member.GenerationID == generationID {
				member.LastHeartbeat = timestamp
				found = true
				break
			} else {
				return ErrIllegalGeneration
			}
		}
	}
	if !found {
		return ErrUnknownMemberID
	}
	return nil
}

func (g *ConsumerGroup) Join(apiVersion int16, memberID string, timestamp time.Time, metadata []protocol.JoinGroupProtocol, sessionTimeoutMs int32, rebalanceTimeoutMs int32) (string, error) {
	g.Lock()
	switch g.state {
	case ConsumerGroupStateStable:
		g.state = ConsumerGroupStateJoining
		g.currentMembers = 0
		if memberID == "" {
			memberID = protocol.NewUUID().String()
			g.members[memberID] = NewConsumerGroupMember(memberID, g.generation+1, timestamp, metadata, sessionTimeoutMs, rebalanceTimeoutMs)
		} else {
			member, exists := g.members[memberID]
			if !exists {
				if apiVersion < 4 {
					g.Unlock()
					return memberID, ErrUnknownMemberID
				}
				member = NewConsumerGroupMember(memberID, g.generation+1, timestamp, metadata, sessionTimeoutMs, rebalanceTimeoutMs)
				g.members[memberID] = member
			}
			member.LastJoin = timestamp
			member.LastHeartbeat = timestamp
		}

		// Wait for the maximum session timeout or otherwise the initial rebalance timeout
		delay := g.initialRebalanceTimeoutMs
		if len(g.members) > 0 {
			for _, member := range g.members {
				if member.isAlive(g.generation, timestamp) {
					if member.RebalanceTimeoutMs > 0 {
						if member.RebalanceTimeoutMs > delay {
							delay = member.RebalanceTimeoutMs
						}
					} else if member.SessionTimeoutMs > 0 && member.SessionTimeoutMs > delay {
						delay = int32(float64(member.SessionTimeoutMs) * 0.9)
					}
				}
			}
		}

		// Lock the group until it unlocks
		g.stateMutex.Lock()

		if g.expectedMembers > 0 {
			g.currentMembers++
			if g.currentMembers >= g.expectedMembers {
				g.leaderElect()
			} else {
				g.joinTimer = time.AfterFunc(time.Millisecond*time.Duration(delay), func() {
					g.Lock()
					defer g.Unlock()
					g.leaderElect()
				})
			}
		} else {
			log.Trace().
				Str("group_id", g.ID).
				Int32("generation_id", g.generation).
				Float64("delay", float64(delay)/1000).
				Msg("Waiting for group members to join")
			g.joinTimer = time.AfterFunc(time.Millisecond*time.Duration(delay), func() {
				g.Lock()
				defer g.Unlock()
				g.leaderElect()
			})
		}

		log.Trace().Str("group_id", g.ID).Int32("generation_id", g.generation+1).Str("member_id", memberID).Int("current_members", g.currentMembers).Int("expected_members", g.expectedMembers).Msg("Rebalance started, leader joining")
		// Wait for state change
		g.Unlock()

		g.stateMutex.Lock()
		g.stateMutex.Unlock()

		log.Trace().Str("group_id", g.ID).Int32("generation_id", g.generation+1).Str("member_id", memberID).Msg("Leader joined")
		// Return leader response
		return memberID, nil

	case ConsumerGroupStateJoining:
		if memberID == "" {
			memberID = protocol.NewUUID().String()
			g.members[memberID] = NewConsumerGroupMember(memberID, g.generation+1, timestamp, metadata, sessionTimeoutMs, rebalanceTimeoutMs)
		} else {
			member, exists := g.members[memberID]
			if !exists {
				if apiVersion < 4 {
					g.Unlock()
					return memberID, ErrUnknownMemberID
				}
				member = NewConsumerGroupMember(memberID, g.generation+1, timestamp, metadata, sessionTimeoutMs, rebalanceTimeoutMs)
				g.members[memberID] = member
			}
			member.LastJoin = timestamp
			member.LastHeartbeat = timestamp
		}

		if g.expectedMembers > 0 {
			g.currentMembers++
			if g.currentMembers >= g.expectedMembers {
				if g.joinTimer.Stop() {
					g.leaderElect()
				}
			}
		}
		log.Trace().Str("group_id", g.ID).Int32("generation_id", g.generation+1).Str("member_id", memberID).Int("current_members", g.currentMembers).Int("expected_members", g.expectedMembers).Msg("Follower joining")

		// Wait for state change
		g.Unlock()
		g.stateMutex.Lock()
		g.stateMutex.Unlock()

		log.Trace().Str("group_id", g.ID).Int32("generation_id", g.generation+1).Str("member_id", memberID).Msg("Follower joined")
		// Return follower response
		return memberID, nil
	case ConsumerGroupStateAwaitSync, ConsumerGroupStateSyncing:
		g.Unlock()
		return memberID, ErrUnknownMemberID
	case ConsumerGroupStateDown:
		g.Unlock()
		return memberID, ErrCoordinatorNotAvailable
	case ConsumerGroupStateInitialize:
		g.Unlock()
		return memberID, ErrCoordinatorLoadInProgress
	}
	panic("Unhandled consumer group state")
}

func (g *ConsumerGroup) Sync(memberID string, generationID int32, assignments []protocol.SyncGroupAssignment, timestamp time.Time) ([]byte, error) {
	g.Lock()

	if generationID > g.generation {
		g.Unlock()
		return nil, ErrIllegalGeneration
	} else if generationID < g.generation {
		g.Unlock()
		return nil, ErrUnknownMemberID
	}

	_, exists := g.members[memberID]
	if !exists {
		g.Unlock()
		return nil, ErrUnknownMemberID
	}

	switch g.state {
	case ConsumerGroupStateAwaitSync:
		g.state = ConsumerGroupStateSyncing
		g.stateMutex.Lock()
		if len(assignments) > 0 {
			g.assignments = assignments
			g.stateMutex.Unlock()
		}
		g.Unlock()

		g.stateMutex.Lock()
		g.stateMutex.Unlock()

		err := g.toStable(memberID)
		if err != nil {
			return nil, err
		}
		for _, assignment := range g.assignments {
			if assignment.MemberID == memberID {
				return assignment.MemberAssignment, nil
			}
		}
		return nil, ErrUnknownMemberID

	case ConsumerGroupStateSyncing:
		if len(assignments) > 0 {
			g.assignments = assignments
			g.stateMutex.Unlock()
		}
		g.Unlock()

		g.stateMutex.Lock()
		g.stateMutex.Unlock()

		err := g.toStable(memberID)
		if err != nil {
			return nil, err
		}

		for _, assignment := range g.assignments {
			if assignment.MemberID == memberID {
				return assignment.MemberAssignment, nil
			}
		}
		return nil, ErrUnknownMemberID
	case ConsumerGroupStateStable:
		g.Unlock()
		return nil, ErrUnknownMemberID
	case ConsumerGroupStateJoining:
		g.Unlock()
		return nil, ErrRebalanceInProgress
	case ConsumerGroupStateDown, ConsumerGroupStateInitialize:
		g.Unlock()
		return nil, ErrCoordinatorNotAvailable
	}
	panic("Unhandled consumer group state")
}

func (g *ConsumerGroup) leaderElect() {
	timestamp := time.Now()

	// Group generation should only be incremented when shifting to await sync as
	// otherwise older generation heartbeats will fail
	g.generation++
	g.findCompatibleAssignmentProtocol(timestamp)
	g.state = ConsumerGroupStateAwaitSync

	leaderElected := false
	for _, member := range g.members {
		if member.GenerationID == g.generation {
			g.leader = member.ID
			leaderElected = true
			break
		}
	}
	if !leaderElected {
		util.Debug("GROUP", g.members)
		// TODO: ERRor
	}
	g.stateMutex.Unlock()
}

func (g *ConsumerGroup) toStable(memberID string) error {
	g.Lock()
	defer g.Unlock()
	member, exists := g.members[memberID]
	if !exists || g.generation != member.GenerationID {
		return ErrUnknownMemberID
	}
	member.Synced = true
	oldMemberIDs := []string{}
	allSynced := true
	for memberID, member := range g.members {
		if member.GenerationID < g.generation {
			oldMemberIDs = append(oldMemberIDs, memberID)
		}
		if member.GenerationID == g.generation && !member.Synced {
			allSynced = false
		}
	}

	// Remove previous generation
	for _, oldMemberID := range oldMemberIDs {
		delete(g.members, oldMemberID)
	}

	if allSynced {
		log.Trace().
			Str("group_id", g.ID).
			Int32("generation_id", g.generation).
			Msg("Consumer group stable")
		g.state = ConsumerGroupStateStable
	}
	return nil
}

func (g *ConsumerGroup) Leave() {

}

func NewConsumerGroup(id string, expectedMembers int) *ConsumerGroup {
	group := &ConsumerGroup{
		ID:                        id,
		generation:                0,
		state:                     ConsumerGroupStateStable,
		members:                   map[string]*ConsumerGroupMember{},
		offsets:                   map[string]map[int32]CommittedOffset{},
		expectedMembers:           expectedMembers,
		initialRebalanceTimeoutMs: 3000,
	}
	return group
}

type ConsumerGroupMember struct {
	ID                 string
	GenerationID       int32
	Metadata           []protocol.JoinGroupProtocol
	GroupInstanceID    string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	Synced             bool
	LastJoin           time.Time
	LastHeartbeat      time.Time
}

func (m *ConsumerGroupMember) isAlive(generationID int32, timestamp time.Time) bool {
	return generationID == m.GenerationID && int32(timestamp.Sub(m.LastHeartbeat).Milliseconds()) < m.SessionTimeoutMs
}

func (m *ConsumerGroupMember) hasJoined(generationID int32, timestamp time.Time) bool {
	return m.GenerationID == generationID && int32(timestamp.Sub(m.LastJoin).Milliseconds()) < m.RebalanceTimeoutMs
}

func (m *ConsumerGroupMember) ProtocolMetadata(name string) ([]byte, error) {
	for _, protocol := range m.Metadata {
		if protocol.Name == name {
			return protocol.Metadata, nil
		}
	}
	return nil, ErrInconsistentGroupProtocol
}

func NewConsumerGroupMember(memberID string, generationID int32, timestamp time.Time, metadata []protocol.JoinGroupProtocol, sessionTimeoutMs int32, rebalanceTimeoutMs int32) *ConsumerGroupMember {
	return &ConsumerGroupMember{
		ID:                 memberID,
		GenerationID:       generationID,
		Metadata:           metadata,
		LastJoin:           timestamp,
		LastHeartbeat:      timestamp,
		Synced:             false,
		SessionTimeoutMs:   sessionTimeoutMs,
		RebalanceTimeoutMs: rebalanceTimeoutMs,
	}
}
