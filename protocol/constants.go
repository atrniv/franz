package protocol

import "encoding/binary"

type APIKeyEnum int16

const (
	APIKeyProduce                     APIKeyEnum = 0
	APIKeyFetch                       APIKeyEnum = 1
	APIKeyListOffset                  APIKeyEnum = 2
	APIKeyMetadata                    APIKeyEnum = 3
	APIKeyLeaderAndIsr                APIKeyEnum = 4
	APIKeyStopReplica                 APIKeyEnum = 5
	APIKeyUpdateMetadata              APIKeyEnum = 6
	APIKeyControlledShutdown          APIKeyEnum = 7
	APIKeyOffsetCommit                APIKeyEnum = 8
	APIKeyOffsetFetch                 APIKeyEnum = 9
	APIKeyFindCoordinator             APIKeyEnum = 10
	APIKeyJoinGroup                   APIKeyEnum = 11
	APIKeyHeartbeat                   APIKeyEnum = 12
	APIKeyLeaveGroup                  APIKeyEnum = 13
	APIKeySyncGroup                   APIKeyEnum = 14
	APIKeyDescribeGroups              APIKeyEnum = 15
	APIKeyListGroups                  APIKeyEnum = 16
	APIKeySaslHandshake               APIKeyEnum = 17
	APIKeyApiVersions                 APIKeyEnum = 18
	APIKeyCreateTopics                APIKeyEnum = 19
	APIKeyDeleteTopics                APIKeyEnum = 20
	APIKeyDeleteRecords               APIKeyEnum = 21
	APIKeyInitProducerId              APIKeyEnum = 22
	APIKeyOffsetForLeaderEpoch        APIKeyEnum = 23
	APIKeyAddPartitionsToTxn          APIKeyEnum = 24
	APIKeyAddOffsetsToTxn             APIKeyEnum = 25
	APIKeyEndTxn                      APIKeyEnum = 26
	APIKeyWriteTxnMarkers             APIKeyEnum = 27
	APIKeyTxnOffsetCommit             APIKeyEnum = 28
	APIKeyDescribeAcls                APIKeyEnum = 29
	APIKeyCreateAcls                  APIKeyEnum = 30
	APIKeyDeleteAcls                  APIKeyEnum = 31
	APIKeyDescribeConfigs             APIKeyEnum = 32
	APIKeyAlterConfigs                APIKeyEnum = 33
	APIKeyAlterReplicaLogDirs         APIKeyEnum = 34
	APIKeyDescribeLogDirs             APIKeyEnum = 35
	APIKeySaslAuthenticate            APIKeyEnum = 36
	APIKeyCreatePartitions            APIKeyEnum = 37
	APIKeyCreateDelegationToken       APIKeyEnum = 38
	APIKeyRenewDelegationToken        APIKeyEnum = 39
	APIKeyExpireDelegationToken       APIKeyEnum = 40
	APIKeyDescribeDelegationToken     APIKeyEnum = 41
	APIKeyDeleteGroups                APIKeyEnum = 42
	APIKeyElectLeaders                APIKeyEnum = 43
	APIKeyIncrementalAlterConfigs     APIKeyEnum = 44
	APIKeyAlterPartitionReassignments APIKeyEnum = 45
	APIKeyListPartitionReassignments  APIKeyEnum = 46
	APIKeyOffsetDelete                APIKeyEnum = 47
)

func (k APIKeyEnum) String() string {
	switch k {
	case APIKeyProduce:
		return "Produce"
	case APIKeyFetch:
		return "Fetch"
	case APIKeyListOffset:
		return "ListOffsets"
	case APIKeyMetadata:
		return "Metadata"
	case APIKeyLeaderAndIsr:
		return "LeaderAndIsr"
	case APIKeyStopReplica:
		return "StopReplica"
	case APIKeyUpdateMetadata:
		return "UpdateMetadata"
	case APIKeyControlledShutdown:
		return "ControlledShutdown"
	case APIKeyOffsetCommit:
		return "OffsetCommit"
	case APIKeyOffsetFetch:
		return "OffsetFetch"
	case APIKeyFindCoordinator:
		return "FindCoordinator"
	case APIKeyJoinGroup:
		return "JoinGroup"
	case APIKeyHeartbeat:
		return "Heartbeat"
	case APIKeyLeaveGroup:
		return "LeaveGroup"
	case APIKeySyncGroup:
		return "SyncGroup"
	case APIKeyDescribeGroups:
		return "DescribeGroups"
	case APIKeyListGroups:
		return "ListGroups"
	case APIKeySaslHandshake:
		return "SaslHandshake"
	case APIKeyApiVersions:
		return "ApiVersions"
	case APIKeyCreateTopics:
		return "CreateTopics"
	case APIKeyDeleteTopics:
		return "DeleteTopics"
	case APIKeyDeleteRecords:
		return "DeleteRecords"
	case APIKeyInitProducerId:
		return "InitProducerId"
	case APIKeyOffsetForLeaderEpoch:
		return "OffsetForLeaderEpoch"
	case APIKeyAddPartitionsToTxn:
		return "AddPartitionsToTxn"
	case APIKeyAddOffsetsToTxn:
		return "AddOffsetsToTxn"
	case APIKeyEndTxn:
		return "EndTxn"
	case APIKeyWriteTxnMarkers:
		return "WriteTxnMarkers"
	case APIKeyTxnOffsetCommit:
		return "TxnOffsetCommit"
	case APIKeyDescribeAcls:
		return "DescribeAcls"
	case APIKeyCreateAcls:
		return "CreateAcls"
	case APIKeyDeleteAcls:
		return "DeleteAcls"
	case APIKeyDescribeConfigs:
		return "DescribeConfigs"
	case APIKeyAlterConfigs:
		return "AlterConfigs"
	case APIKeyAlterReplicaLogDirs:
		return "AlterReplicaLogDirs"
	case APIKeyDescribeLogDirs:
		return "DescribeLogDirs"
	case APIKeySaslAuthenticate:
		return "SaslAuthenticate"
	case APIKeyCreatePartitions:
		return "CreatePartitions"
	case APIKeyCreateDelegationToken:
		return "CreateDelegationToken"
	case APIKeyRenewDelegationToken:
		return "RenewDelegationToken"
	case APIKeyExpireDelegationToken:
		return "ExpireDelegationToken"
	case APIKeyDescribeDelegationToken:
		return "DescribeDelegationToken"
	case APIKeyDeleteGroups:
		return "DeleteGroups"
	case APIKeyElectLeaders:
		return "ElectLeaders"
	case APIKeyIncrementalAlterConfigs:
		return "IncrementalAlterConfigs"
	case APIKeyAlterPartitionReassignments:
		return "AlterPartitionReassignments"
	case APIKeyListPartitionReassignments:
		return "ListPartitionReassignments"
	case APIKeyOffsetDelete:
		return "OffsetDelete"
	default:
		return "Unknown"
	}
}

type KeyTypeEnum int8

const (
	KeyTypeGroup       KeyTypeEnum = 0
	KeyTypeTransaction KeyTypeEnum = 1
)

type CompressionCodec int8

func (cc CompressionCodec) String() string {
	switch cc {
	case 0:
		return "none"
	case 1:
		return "gzip"
	case 2:
		return "snappy"
	case 3:
		return "lz4"
	case 4:
		return "ztd"
	default:
		return ""
	}
}

const (
	//CompressionNone no compression
	CompressionNone CompressionCodec = iota
	//CompressionGZIP compression using GZIP
	CompressionGZIP
	//CompressionSnappy compression using snappy
	CompressionSnappy
	//CompressionLZ4 compression using LZ4
	CompressionLZ4
	//CompressionZSTD compression using ZSTD
	CompressionZSTD

	CompressionLevelDefault      = -1000
	compressionCodecMask    int8 = 0x07
	timestampTypeMask            = 0x08
	isTransactionalMask          = 0x10
	controlMask                  = 0x20
	recordBatchOverhead          = 49
	maximumRecordOverhead        = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1
)

type TimestampType int8

const (
	CreateTime TimestampType = iota
	LogAppendTime
)

func (t TimestampType) String() string {
	switch t {
	case 0:
		return "CreateTime"
	case 1:
		return "LogAppendTime"
	default:
		return ""
	}
}
