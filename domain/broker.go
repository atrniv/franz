package domain

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/atrniv/franz/protocol"
	"github.com/atrniv/franz/util"
	"github.com/rs/zerolog/log"
)

var nextBrokerID int32 = 0

type Broker struct {
	ID      int32
	Cluster *Cluster

	Host           string
	Port           int32
	consumerGroups map[string]*ConsumerGroup

	sync.RWMutex

	nextConnectionID int64
	context          context.Context
	cancel           context.CancelFunc
	listener         net.Listener
	connections      map[int64]*connection
	sl               *sync.RWMutex
	wg               *sync.WaitGroup
}

type connection struct {
	ID      int64
	Addr    string
	conn    net.Conn
	buffer  *bufio.Reader
	context context.Context
	closed  bool
	active  bool
	sync.RWMutex
}

func (c *connection) markActive(active bool) {
	c.Lock()
	c.active = active
	c.Unlock()
}

func (c *connection) close() {
	for {
		c.Lock()
		if !c.active && !c.closed {
			c.closed = true
			err := c.conn.Close()
			if err != nil {
				log.Error().
					Int64("conn_id", c.ID).
					Err(err).
					Msg("Connection close error")
			}
			c.Unlock()
			break
		} else if c.closed {
			c.Unlock()
			break
		} else {
			log.Error().Int64("conn_id", c.ID).Msg("Connection busy")
		}
		c.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (b *Broker) Start(addr string, expectedConsumerGroupMembers map[string]int, waitForConsumers bool) error {
	var err error

	b.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for groupID, expectedMembers := range expectedConsumerGroupMembers {
		b.BecomeGroupCoordinator(groupID, expectedMembers)
	}

	// Parse host and port
	parts := strings.Split(addr, ":")
	b.Host = parts[0]
	v64, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return err
	}
	b.Port = int32(v64)

	go b.serve()

	log.Debug().Str("cluster_id", b.Cluster.ID).Int32("broker_id", b.ID).Msg("Broker has started accepting connections")

	if expectedConsumerGroupMembers != nil && waitForConsumers {
		log.Info().Str("cluster_id", b.Cluster.ID).Int32("broker_id", b.ID).Msg("Waiting for consumers to join group")
		if !b.waitForConsumerGroups() {
			err := fmt.Errorf("Kafka consumer failed to join group")
			log.Error().Err(err).Str("cluster_id", b.Cluster.ID).Int32("broker_id", b.ID).Msg("Kafka setup failed")
			return err
		}
		log.Info().Str("cluster_id", b.Cluster.ID).Int32("broker_id", b.ID).Msg("All consumers ready")
	}

	return nil
}

func (b *Broker) waitForConsumerGroups() bool {
	retries := 20
	for {
		ready := true
		b.RLock()
		for _, consumerGroup := range b.consumerGroups {
			if !consumerGroup.IsReady() {
				ready = false
			}
		}
		b.RUnlock()
		retries--
		if ready || retries < 0 {
			return ready
		}
		time.Sleep(time.Second)
	}
}

func (b *Broker) Stop() error {
	b.sl.Lock()
	b.cancel()
	b.listener.Close()

	connections := []*connection{}
	for _, connection := range b.connections {
		connections = append(connections, connection)
	}
	b.sl.Unlock()

	b.Reset()
	for _, connection := range connections {
		b.closeConnection(connection)
	}

	b.wg.Wait()

	log.Debug().Str("cluster_id", b.Cluster.ID).Int32("broker_id", b.ID).Msg("Broker shutdown")
	return nil
}

func (b *Broker) Lag(groupIDs []string, offsets map[string]map[int32]int64) int64 {
	b.RLock()
	defer b.RUnlock()
	lag := int64(0)
	for consumerGroupID, consumerGroup := range b.consumerGroups {
		exists := false
		for _, groupID := range groupIDs {
			if consumerGroupID == groupID {
				exists = true
				break
			}
		}

		if len(groupIDs) == 0 || exists {
			for topic, partitions := range offsets {
				for partition, lastOffset := range partitions {
					committedOffset := consumerGroup.CommittedOffset(topic, partition)
					lag += lastOffset - committedOffset
					// println("TOPIC ", topic, partition, " LAG ", lastOffset, committedOffset, lag)
				}
			}
			break
		}
	}
	return lag
}

func (b *Broker) closeConnection(c *connection) {
	c.close()

	b.sl.Lock()
	delete(b.connections, c.ID)
	active := len(b.connections)
	b.sl.Unlock()

	log.Info().
		Int64("conn_id", c.ID).
		Str("remote_addr", c.Addr).
		Int("active", active).
		Msg("Connection closed")
}

func (b *Broker) handleConnection(c *connection) {
	log.Trace().Int64("conn_id", c.ID).Str("remote_addr", c.Addr).Msg("Serving connection")
LOOP:
	for {
		select {
		case <-c.context.Done():
			b.closeConnection(c)
			break LOOP
		default:
			closed := b.handleRequest(c)
			if closed {
				b.closeConnection(c)
				break LOOP
			}
		}
	}
}

func (b *Broker) serve() {
	b.wg.Add(1)
	defer b.wg.Done()
	for {
		tc, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.context.Done():
				log.Debug().Msg("Broker has stopped accepting new connections")
				return
			default:
				log.Error().Err(err).Msg("An error occurred while accepting a new connection")
			}
		} else {
			b.sl.Lock()
			active := len(b.connections)
			b.nextConnectionID++
			conn := &connection{
				ID:      b.nextConnectionID,
				Addr:    tc.RemoteAddr().String(),
				conn:    tc,
				buffer:  bufio.NewReader(tc),
				context: b.context,
			}
			b.connections[conn.ID] = conn
			active++
			b.sl.Unlock()

			log.Info().Int64("conn_id", conn.ID).Str("remote_addr", conn.Addr).Int("active", active).Msg("Connection established")
			go func() {
				b.wg.Add(1)
				defer b.wg.Done()
				b.handleConnection(conn)
			}()
		}
	}

}

func (b *Broker) Reset() {
	b.Lock()
	for _, consumerGroup := range b.consumerGroups {
		consumerGroup.Reset()
	}
	b.Unlock()
}

func (b *Broker) proxyConnection(serverAddr string, c *connection) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	if err != nil {
		os.Exit(1)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		os.Exit(1)
	}
LOOP:
	for {
		select {
		case <-c.context.Done():
			conn.Close()
			b.closeConnection(c)
			break LOOP
		default:
			closed := b.proxyRequest(conn, c)
			if closed {
				conn.Close()
				b.closeConnection(c)
				break LOOP
			}

		}
	}
}

func (b *Broker) proxyRequest(serverConn *net.TCPConn, c *connection) bool {
	serverReq, err := protocol.NewTCPMessage(c.conn)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Trace().
				Int64("conn_id", c.ID).
				Str("remote_addr", c.Addr).
				Msg("Closing connection")
			return true
		} else {
			log.Error().
				Int64("conn_id", c.ID).
				Str("remote_addr", c.Addr).
				Err(err).
				Msg("Connection error while reading message")
			return true
		}
	} else {
		reader := protocol.NewReader(serverReq)

		apiKey, apiVersion := reader.APIMeta()
		header, err := protocol.ReadRequestHeader(reader, apiVersion)
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				// Str("remote_addr", c.Addr).
				Str("client_id", header.ClientID.Value).
				Err(err).
				Msg("Connection error")
			return true
		}
		log.Debug().
			Int64("conn_id", c.ID).
			Str("client_id", header.ClientID.Value).
			Str("api", apiKey.String()).
			Int16("version", apiVersion).
			Int32("correlation_id", header.CorrelationID).
			Msg("REQ RCV")

		err = serverReq.Write(serverConn)
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Err(err).
				Msg("Error sending message to server")
			return true
		}

		serverRes, err := protocol.NewTCPMessage(serverConn)
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Err(err).
				Msg("Error receiving response from server")
			return true
		}

		b.debugRequestResponse(serverReq, serverRes)

		err = serverRes.Write(c.conn)
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Err(err).
				Msg("Error sending response to client")
			return true
		}
	}
	return false
}

func (b *Broker) debugRequestResponse(serverReq protocol.TCPMessage, serverRes protocol.TCPMessage) {
	reqReader := protocol.NewReader(serverReq)
	resReader := protocol.NewReader(serverRes)
	apiKey, apiVersion := reqReader.APIMeta()

	header, err := protocol.ReadRequestHeader(reqReader, apiVersion)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while parsing the req header")
	}

	switch apiKey {
	case protocol.APIKeyJoinGroup:
		req, err := protocol.ReadJoinGroupRequest(reqReader, apiVersion)
		if err != nil {
			log.Error().Err(err).Msg("An error occurred while parsing the response")
		}
		util.Debug(fmt.Sprintf("REQ %s (%s) - %d ", apiKey.String(), header.ClientID.Value, header.CorrelationID), req)

	case protocol.APIKeyListOffset:
		req, err := protocol.ReadListOffsetRequest(reqReader, apiVersion)
		if err != nil {
			log.Error().Err(err).Msg("An error occurred while parsing the response")
		}
		util.Debug(fmt.Sprintf("REQ %s (%s) - %d ", apiKey.String(), header.ClientID.Value, header.CorrelationID), req)

		res, resHeader, err := protocol.ReadListOffsetResponse(apiVersion, resReader)
		util.Debug(fmt.Sprintf("RES %s (%s) - %d ", apiKey.String(), header.ClientID.Value, resHeader.CorrelationID), res)
		if err != nil {
			log.Error().Err(err).Msg("An error occurred while parsing the response")
		}

	case protocol.APIKeyFetch:
		req, err := protocol.ReadFetchRequest(reqReader, apiVersion)
		if err != nil {
			log.Error().Err(err).Msg("An error occurred while parsing the response")
		}
		util.Debug(fmt.Sprintf("REQ %s (%s) - %d ", apiKey.String(), header.ClientID.Value, header.CorrelationID), req)

		res, resHeader, err := protocol.ReadFetchResponse(apiVersion, resReader)
		util.Debug(fmt.Sprintf("RES %s (%s) - %d ", apiKey.String(), header.ClientID.Value, resHeader.CorrelationID), res)
		if err != nil {
			log.Error().Err(err).Msg("An error occurred while parsing the response")
		}

		messageVersion := int8(0)
		if apiVersion >= 4 {
			messageVersion = 2
		} else if apiVersion >= 2 {
			messageVersion = 1
		}
		for _, topic := range res.Topics {
			for _, partition := range topic.Partitions {
				recData := protocol.ReadRecordData(messageVersion, protocol.NewReader(partition.Records))
				util.Debug(topic.Name+", "+strconv.FormatInt(int64(partition.PartitionIndex), 10), recData)
			}
		}
		if err != nil {
			log.Error().Err(err).Msg("An error occurred while parsing the response")
		}
	}
}

func (b *Broker) handleRequest(c *connection) bool {
	message, err := protocol.NewTCPMessage(c.buffer)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Trace().
				Int64("conn_id", c.ID).
				Str("remote_addr", c.Addr).
				Msg("Closing connection")
			return true
		} else {
			log.Error().
				Int64("conn_id", c.ID).
				Str("remote_addr", c.Addr).
				Err(err).
				Msg("Connection error while reading message")
			return true
		}
	} else {
		c.markActive(true)
		defer c.markActive(false)

		reader := protocol.NewReader(message)

		apiKey, apiVersion := reader.APIMeta()
		header, err := protocol.ReadRequestHeader(reader, apiVersion)
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Err(err).
				Msg("Connection error")
			return true
		}

		log.Debug().
			Int64("conn_id", c.ID).
			Str("client_id", header.ClientID.Value).
			Str("api", apiKey.String()).
			Int16("version", apiVersion).
			Int32("correlation_id", header.CorrelationID).
			Msg("REQ RCV")

		resBuffer := bytes.NewBuffer([]byte{})
		switch apiKey {
		case protocol.APIKeyProduce:
			err = b.handleProduceRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyFetch:
			err = b.handleFetchRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyListOffset:
			err = b.handleListOffsetsRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyOffsetCommit:
			err = b.handleOffsetCommitRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyOffsetFetch:
			err = b.handleOffsetFetchRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyHeartbeat:
			err = b.handleHeartbeatRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeySyncGroup:
			err = b.handleSyncGroupRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyJoinGroup:
			err = b.handleJoinGroupRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyFindCoordinator:
			err = b.handleFindCoordinatorRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyLeaveGroup:
		case protocol.APIKeyApiVersions:
			err = b.handleAPIVersionsRequest(apiVersion, header, reader, resBuffer)
		case protocol.APIKeyMetadata:
			err = b.handleMetadataRequest(apiVersion, header, reader, resBuffer)
		default:
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Str("api", apiKey.String()).
				Int16("version", apiVersion).
				Msg("Not handled")
			panic("Shutting down server")
		}
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Err(err).
				Msg("Error encountered while processing message")
			return true
		}

		bytesWritten, err := c.conn.Write(resBuffer.Bytes())
		if err != nil {
			log.Error().
				Int64("conn_id", c.ID).
				Str("client_id", header.ClientID.Value).
				Str("api", apiKey.String()).
				Int16("version", apiVersion).
				Int32("correlation_id", header.CorrelationID).
				Err(err).
				Msg("Connection write error")
			return true
		}
		log.Debug().
			Int64("conn_id", c.ID).
			Str("client_id", header.ClientID.Value).
			Str("api", apiKey.String()).
			Int16("version", apiVersion).
			Int("bytes_written", bytesWritten).
			Int32("correlation_id", header.CorrelationID).
			Msg("RES SND")
	}
	return false
}

func (b *Broker) IsGroupCoordinator(groupID string) bool {
	b.RLock()
	_, ok := b.consumerGroups[groupID]
	b.RUnlock()
	return ok
}

func (b *Broker) BecomeGroupCoordinator(groupID string, expectedMembers int) {
	b.Lock()
	b.consumerGroups[groupID] = NewConsumerGroup(groupID, expectedMembers)
	b.Unlock()
}

func (b *Broker) GetConsumerGroup(groupID string) (*ConsumerGroup, bool) {
	b.RLock()
	group, exists := b.consumerGroups[groupID]
	b.RUnlock()
	return group, exists
}

func (b *Broker) handleAPIVersionsRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	_, err := protocol.ReadAPIVersionRequest(r, apiVersion)
	if err != nil {
		return err
	}
	// util.Debug("API VERSIONS REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.APIVersionsResponse{
		APIKeys: []protocol.APIVersionsResponseKey{
			{APIKey: int16(protocol.APIKeyProduce), MinVersion: 0, MaxVersion: 7},
			{APIKey: int16(protocol.APIKeyFetch), MinVersion: 0, MaxVersion: 10},
			{APIKey: int16(protocol.APIKeyListOffset), MinVersion: 0, MaxVersion: 5},
			{APIKey: int16(protocol.APIKeyMetadata), MinVersion: 0, MaxVersion: 7},
			{APIKey: int16(protocol.APIKeyLeaderAndIsr), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyStopReplica), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyUpdateMetadata), MinVersion: 0, MaxVersion: 5},
			{APIKey: int16(protocol.APIKeyControlledShutdown), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyOffsetCommit), MinVersion: 0, MaxVersion: 6},
			{APIKey: int16(protocol.APIKeyOffsetFetch), MinVersion: 0, MaxVersion: 5},
			{APIKey: int16(protocol.APIKeyFindCoordinator), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyJoinGroup), MinVersion: 0, MaxVersion: 4},
			{APIKey: int16(protocol.APIKeyHeartbeat), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyLeaveGroup), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeySyncGroup), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyDescribeGroups), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyListGroups), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeySaslHandshake), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyApiVersions), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyCreateTopics), MinVersion: 0, MaxVersion: 3},
			{APIKey: int16(protocol.APIKeyDeleteTopics), MinVersion: 0, MaxVersion: 20},
			{APIKey: int16(protocol.APIKeyDeleteRecords), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyInitProducerId), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyOffsetForLeaderEpoch), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyAddPartitionsToTxn), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyAddOffsetsToTxn), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyEndTxn), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyWriteTxnMarkers), MinVersion: 0, MaxVersion: 0},
			{APIKey: int16(protocol.APIKeyTxnOffsetCommit), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyDescribeAcls), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyCreateAcls), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyDeleteAcls), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyDescribeConfigs), MinVersion: 0, MaxVersion: 2},
			{APIKey: int16(protocol.APIKeyAlterConfigs), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyAlterReplicaLogDirs), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyDescribeLogDirs), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeySaslAuthenticate), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyCreatePartitions), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyCreateDelegationToken), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyRenewDelegationToken), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyExpireDelegationToken), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyDescribeDelegationToken), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyDeleteGroups), MinVersion: 0, MaxVersion: 1},
			{APIKey: int16(protocol.APIKeyElectLeaders), MinVersion: 0, MaxVersion: 0},
			{APIKey: int16(protocol.APIKeyIncrementalAlterConfigs), MinVersion: 0, MaxVersion: 0},
			{APIKey: int16(protocol.APIKeyAlterPartitionReassignments), MinVersion: 0, MaxVersion: 0},
			{APIKey: int16(protocol.APIKeyListPartitionReassignments), MinVersion: 0, MaxVersion: 0},
			{APIKey: int16(protocol.APIKeyOffsetDelete), MinVersion: 0, MaxVersion: 0},
		},
	}

	// util.Debug("API VERSIONS RES", res)

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}
	return nil

}

func (b *Broker) handleProduceRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadProduceRequest(apiVersion, r)
	if err != nil {
		return err
	}

	// util.Debug("PRODUCE REQ", req)
	writer := protocol.NewWriter(w)
	res := protocol.ProduceResponse{
		Responses: make([]protocol.TopicProduceResponse, len(req.Topics)),
	}

	for index, topic := range req.Topics {
		clusterTopic, err := b.Cluster.GetTopic(topic.Name)
		if err != nil {
			// TODO: handle error
		}
		topicResponse := protocol.TopicProduceResponse{
			Name:       topic.Name,
			Partitions: make([]protocol.PartitionProduceResponse, len(topic.Partitions)),
		}
		for index, partition := range topic.Partitions {
			clusterPartition, err := clusterTopic.GetPartition(partition.PartitionIndex)
			if err != nil {
				// TODO: handle err
			}
			baseOffset, logAppendTime, logStartOffset, err := clusterPartition.AppendMessages(apiVersion, partition.Records)
			if err != nil {
				// TODO:  handle err
			} else {
				partitionResponse := protocol.PartitionProduceResponse{
					PartitionIndex:  partition.PartitionIndex,
					BaseOffset:      baseOffset,
					LogAppendTimeMs: logAppendTime,
					LogStartOffset:  logStartOffset,
				}
				topicResponse.Partitions[index] = partitionResponse
			}
		}
		res.Responses[index] = topicResponse
	}

	// util.Debug("PRODUCE RES", res)

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleFetchRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	startTime := time.Now()
	req, err := protocol.ReadFetchRequest(r, apiVersion)
	if err != nil {
		return err
	}
	endTime := startTime.Add(time.Duration(req.MaxWait) * time.Millisecond)

	// util.Debug("FETCH REQUEST", req)

	writer := protocol.NewWriter(w)

	hasData := false
	for !hasData {
		res := protocol.FetchResponse{
			SessionID: req.SessionID,
			Topics:    make([]protocol.FetchableTopicResponse, len(req.Topics)),
		}
		totalSize := int32(0)
		for index, topic := range req.Topics {
			topicResponse := protocol.FetchableTopicResponse{
				Name:       topic.Name,
				Partitions: make([]protocol.FetchablePartitionResponse, len(topic.FetchPartitions)),
			}
			clusterTopic, err := b.Cluster.GetTopic(topic.Name)
			if err != nil {
				println("FETCH TOPIC EXIT")
				// TODO: Return err
			}

			for index, partition := range topic.FetchPartitions {
				partitionResponse := protocol.FetchablePartitionResponse{
					PartitionIndex: partition.PartitionIndex,
				}
				clusterPartition, err := clusterTopic.GetPartition(partition.PartitionIndex)
				if err != nil {
					println("FETCH PARTITION EXIT")
					// TODO: Return err
				}

				records, recordCount, newTotalSize, logStartOffset, lastStableOffset, highWatermark, err := clusterPartition.FetchMessages(apiVersion, partition.FetchOffset, totalSize, partition.LogStartOffset, partition.CurrentLeaderEpoch, partition.MaxBytes, endTime)
				if err != nil {
					println("FETCH PAR DATA EXIT")
					// TODO: Return err
				}

				totalSize = newTotalSize

				if recordCount > 0 {
					hasData = true
				}
				// Serialize messages to correct format
				partitionResponse.PreferredReadReplica = b.ID
				partitionResponse.LogStartOffset = logStartOffset
				partitionResponse.LastStableOffset = lastStableOffset
				partitionResponse.HighWatermark = highWatermark
				partitionResponse.Records = records
				topicResponse.Partitions[index] = partitionResponse
			}
			res.Topics[index] = topicResponse
		}
		elaspedTime := time.Now().Sub(startTime)
		if !hasData && elaspedTime < time.Duration(req.MaxWait)*time.Millisecond {
			time.Sleep(100 * time.Millisecond)
		} else {
			err = res.Write(apiVersion, header, writer)
			if err != nil {
				return err
			}
			break
		}
	}

	return nil
}

func (b *Broker) handleListOffsetsRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadListOffsetRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("LIST OFFSET REQUEST", req)

	writer := protocol.NewWriter(w)

	res := protocol.ListOffsetResponse{}

	if apiVersion >= 1 {
		res.Topics = make([]protocol.ListOffsetTopicResponse, len(req.Topics))
		for index, topic := range req.Topics {
			clusterTopic, err := b.Cluster.GetTopic(topic.Name)
			if err != nil {
				// TODO: Send Error
				return err
			}
			listOffsetTopicResponse := protocol.ListOffsetTopicResponse{
				Name:       topic.Name,
				Partitions: make([]protocol.ListOffsetPartitionResponse, len(topic.Partitions)),
			}
			for index, partition := range topic.Partitions {
				clusterTopicPartition, err := clusterTopic.GetPartition(partition.PartitionIndex)
				if err != nil {
					return err
				}
				listOffsetPartitionResponse := protocol.ListOffsetPartitionResponse{
					PartitionIndex: partition.PartitionIndex,
				}
				listOffsetPartitionResponse.Offset, listOffsetPartitionResponse.Timestamp = clusterTopicPartition.GetOffset(partition.Timestamp)
				listOffsetTopicResponse.Partitions[index] = listOffsetPartitionResponse
			}
			res.Topics[index] = listOffsetTopicResponse
		}
	}
	// util.Debug("LIST OFFSET RES", res)

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	return nil
}
func (b *Broker) handleOffsetCommitRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadOffsetCommitRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("OFFSET COMMIT REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.OffsetCommitResponse{}
	res.Topics = make([]protocol.OffsetCommitResponseTopic, len(req.Topics))

	var lastErr error
	consumerGroup, exists := b.GetConsumerGroup(req.GroupID)
	if !exists {
		lastErr = ErrNotCoordinator
	} else {
		members := consumerGroup.Members()
		_, exists = members[req.MemberID]
		if !exists {
			lastErr = ErrUnknownMemberID
		} else if req.GenerationID != consumerGroup.Generation() {
			lastErr = ErrIllegalGeneration
		}
	}

	for index, topic := range req.Topics {
		topicResponse := protocol.OffsetCommitResponseTopic{
			Name:       topic.Name,
			Partitions: make([]protocol.OffsetCommitResponsePartition, len(topic.Partitions)),
		}
		clusterTopic, err := b.Cluster.GetTopic(topic.Name)
		if err != nil && lastErr != nil {
			lastErr = err
		}
		for index, partition := range topic.Partitions {
			if clusterTopic != nil {
				clusterPartition, err := clusterTopic.GetPartition(partition.PartitionIndex)
				if err != nil && lastErr != nil {
					lastErr = err
				}
				if partition.CommittedLeaderEpoch != -1 && partition.CommittedLeaderEpoch != clusterPartition.LeaderEpoch && lastErr != nil {
					lastErr = ErrFencedInstanceID
				}
				if lastErr == nil {
					consumerGroup.CommitOffset(topic.Name, partition.PartitionIndex, partition.CommittedOffset, partition.CommittedLeaderEpoch, partition.CommitTimestamp, partition.CommittedMetadata)
				}
			}
			errorCode := int16(0)
			var exception KafkaException
			if lastErr != nil {
				if errors.As(lastErr, &exception) {
					errorCode = exception.Code
				} else {
					return lastErr
				}
			}
			topicResponse.Partitions[index] = protocol.OffsetCommitResponsePartition{
				PartitionIndex: partition.PartitionIndex,
				ErrorCode:      errorCode,
			}
		}
		res.Topics[index] = topicResponse
	}

	// util.Debug("OFFSET COMMIT RES", res)

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleOffsetFetchRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadOffsetFetchRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("OFFSET FETCH REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.OffsetFetchResponse{}

	consumerGroup, exists := b.GetConsumerGroup(req.GroupID)
	if !exists {
		res.ErrorCode = ErrCoordinatorNotAvailable.Code
		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
		// util.Debug("OFFSET FETCH RES ERR", res)
		return nil
	}

	res.Topics = make([]protocol.OffsetFetchTopicResponse, len(req.Topics))
	for index, topic := range req.Topics {
		topicResponse := protocol.OffsetFetchTopicResponse{
			Name:       topic.Name,
			Partitions: make([]protocol.OffsetFetchTopicPartitionResponse, len(topic.PartitionIndexes)),
		}
		clusterTopic, err := b.Cluster.GetTopic(topic.Name)
		if err != nil {
			return err
		}
		for index, partitionIndex := range topic.PartitionIndexes {
			clusterPartition, err := clusterTopic.GetPartition(partitionIndex)
			if err != nil {
				return err
			}
			epoch := clusterPartition.LeaderEpoch
			offset := consumerGroup.CommittedOffset(topic.Name, partitionIndex)
			partitionResponse := protocol.OffsetFetchTopicPartitionResponse{
				PartitionIndex:       partitionIndex,
				CommittedOffset:      offset,
				CommittedLeaderEpoch: epoch,
				Metadata:             protocol.NullableString{IsNull: true, IsValid: true},
				ErrorCode:            0,
			}
			topicResponse.Partitions[index] = partitionResponse
		}
		res.Topics[index] = topicResponse
	}

	// TODO: Check that the heartbeat is valid
	// util.Debug("OFFSET FETCH RES", res)

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleHeartbeatRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadHearbeatRequest(r, apiVersion)
	if err != nil {
		return err
	}
	timestamp := time.Now()
	// util.Debug("HEARTBEAT REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.HearbeatResponse{}

	consumerGroup, exists := b.GetConsumerGroup(req.GroupID)
	if !exists {
		res.ErrorCode = ErrNotCoordinator.Code
		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
		return nil
	}

	err = consumerGroup.Heartbeat(req.MemberID, req.GenerationID, timestamp)
	var exception KafkaException
	if errors.As(err, &exception) {
		res.ErrorCode = exception.Code
		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
		return nil
	}

	// util.Debug("HEARTBEAT RES", res)

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleSyncGroupRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadSyncGroupRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("SYNC GROUP REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.SyncGroupResponse{}

	consumerGroup, exists := b.GetConsumerGroup(req.GroupID)
	if !exists {
		res.ErrorCode = ErrNotCoordinator.Code
		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
		return nil
	}
	timestamp := time.Now()
	assignment, err := consumerGroup.Sync(req.MemberID, req.GenerationID, req.GroupAssigments, timestamp)
	if err != nil {
		var exception KafkaException
		if errors.As(err, &exception) {
			res.ErrorCode = exception.Code
			// util.Debug("SYNC GROUP ERR", res)
			err = res.Write(apiVersion, header, writer)
			if err != nil {
				return err
			}
			return nil
		}
	}
	res.MemberAssignment = assignment

	// util.Debug("SYNC GROUP RES", res)
	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleJoinGroupRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	timestamp := time.Now()

	req, err := protocol.ReadJoinGroupRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("JOIN GROUP REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.JoinGroupResponse{
		Members: []protocol.JoinGroupMember{},
	}

	consumerGroup, exists := b.GetConsumerGroup(req.GroupID)
	if !exists {
		res.ErrorCode = ErrNotCoordinator.Code
		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
		return nil
	}

	if apiVersion >= 4 && req.MemberID == "" {

		// Send a member id with an error
		res.MemberID = protocol.NewUUID().String()
		res.ErrorCode = ErrMemberIDRequired.Code
		res.GenerationID = consumerGroup.Generation()

		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
	} else {
		// Send a member id
		memberID, err := consumerGroup.Join(apiVersion, req.MemberID, timestamp, req.Protocols, req.SessionTimeoutMs, req.RebalanceTimeoutMs)
		if err != nil {
			var exception KafkaException
			if errors.As(err, &exception) {
				res.ErrorCode = exception.Code
				err = res.Write(apiVersion, header, writer)
				if err != nil {
					return err
				}
				return nil
			}
			return err
		}

		if consumerGroup.AssignmentProtocol() == "" {
			res.ErrorCode = ErrInconsistentGroupProtocol.Code
			err = res.Write(apiVersion, header, writer)
			if err != nil {
				return err
			}
			return nil
		}

		res.GenerationID = consumerGroup.Generation()
		res.Leader = consumerGroup.Leader()
		res.MemberID = memberID
		res.ProtocolType = protocol.NullableString{
			IsValid: true,
			Value:   "consumer",
		}
		res.ProtocolName = protocol.NullableString{
			IsValid: true,
			Value:   consumerGroup.AssignmentProtocol(),
		}

		if memberID == res.Leader {
			for _, member := range consumerGroup.members {
				if member.GenerationID != res.GenerationID {
					continue
				}
				groupInstanceID := protocol.NullableString{IsNull: true, IsValid: true}
				if member.GroupInstanceID != "" {
					groupInstanceID = protocol.NullableString{IsValid: true, Value: member.GroupInstanceID}
				}
				metadata, err := member.ProtocolMetadata(consumerGroup.assignmentProtocol)
				if err != nil {
					var exception KafkaException
					if errors.As(err, &exception) {
						res.ErrorCode = exception.Code
						err = res.Write(apiVersion, header, writer)
						if err != nil {
							return err
						}
						return nil
					}
				}
				res.Members = append(res.Members, protocol.JoinGroupMember{
					MemberID:        member.ID,
					Metadata:        metadata,
					GroupInstanceID: groupInstanceID,
				})
			}
		} else {
			res.Members = []protocol.JoinGroupMember{}
		}

		// util.Debug("JOIN GROUP RES", res)

		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) handleFindCoordinatorRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadFindCoordinatorRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("FIND COORDINATOR REQUEST", req)

	if req.KeyType == protocol.KeyTypeGroup {

		// Lock the cluster to ensure we always find the same co-ordinator for a group
		brokers := b.Cluster.GetBrokers()

		found := false
		var coordinator *Broker
		for _, broker := range brokers {
			if broker.IsGroupCoordinator(req.Key) {
				found = true
				coordinator = broker
				break
			}
		}

		if !found {
			coordinator = b
			coordinator.BecomeGroupCoordinator(req.Key, -1)
		}
		writer := protocol.NewWriter(w)
		res := protocol.FindCoordinatorResponse{
			ErrorCode:    0,
			ErrorMessage: protocol.NullableString{IsNull: true, IsValid: true},
			NodeID:       coordinator.ID,
			Host:         coordinator.Host,
			Port:         coordinator.Port,
		}

		// util.Debug("FIND COORDINATOR RES", res)
		err = res.Write(apiVersion, header, writer)
		if err != nil {
			return err
		}
	}

	return nil
}
func (b *Broker) handleMetadataRequest(apiVersion int16, header protocol.RequestHeader, r *protocol.Reader, w io.Writer) error {
	req, err := protocol.ReadMetadataRequest(r, apiVersion)
	if err != nil {
		return err
	}

	// util.Debug("METADATA REQUEST", req)

	writer := protocol.NewWriter(w)
	res := protocol.MetadataResponse{
		Brokers: []protocol.BrokerMetadata{
			{
				NodeID: b.ID,
				Host:   b.Host,
				Port:   b.Port,
				Rack:   protocol.NullableString{IsNull: true},
			},
		},
		Topics:       []protocol.TopicMetadata{},
		ControllerID: b.ID,
		ClusterID: protocol.NullableString{
			IsValid: true,
			Value:   b.Cluster.ID,
		},
	}
	if req.AllowTopicAutoCreation {
		// Create topics that don't exist
		for _, topicName := range req.Topics {
			_, err := b.Cluster.GetTopic(topicName)
			if err == ErrUnknownTopicOrPartition {
				_, err = b.Cluster.CreateTopic(topicName, 1)
				if err != nil {
					return err
				}
			}
		}
	}

	topics := b.Cluster.GetTopics()

	for _, topic := range topics {
		exists := false
		for _, topicName := range req.Topics {
			if topic.Name == topicName {
				exists = true
				break
			}
		}
		if !exists && !req.AllTopicsMetadata {
			continue
		}

		topicMeta := protocol.TopicMetadata{
			ErrorCode:                 0,
			IsInternal:                false,
			Name:                      topic.Name,
			TopicAuthorizedOperations: 0,
			Partitions:                []protocol.PartitionMetadata{},
		}
		for index, partition := range topic.Partitions {
			partitionMeta := protocol.PartitionMetadata{
				PartitionIndex: int32(index),
				ErrorCode:      0,
				LeaderID:       partition.LeaderID,
				LeaderEpoch:    partition.LeaderEpoch,
				ISRNodes:       partition.ISRNodes,
				ReplicaNodes:   partition.ReplicaNodes,
			}
			topicMeta.Partitions = append(topicMeta.Partitions, partitionMeta)
		}
		res.Topics = append(res.Topics, topicMeta)
	}

	err = res.Write(apiVersion, header, writer)
	if err != nil {
		return err
	}

	// util.Debug("METADATA RESPONSE", res)

	return nil
}

func NewBroker(c *Cluster) *Broker {
	nextBrokerID++
	b := &Broker{
		ID:             nextBrokerID,
		Cluster:        c,
		consumerGroups: map[string]*ConsumerGroup{},
		connections:    map[int64]*connection{},
		sl:             &sync.RWMutex{},
		wg:             &sync.WaitGroup{},
	}
	b.context, b.cancel = context.WithCancel(context.Background())
	return b
}
