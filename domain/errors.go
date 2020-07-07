package domain

var ErrUnknownServerError = NewKafkaException(-1, "UNKNOWN_SERVER_ERROR", "The server experienced an unexpected error when processing the request.")
var ErrOffsetOutOfRange = NewKafkaException(1, "OFFSET_OUT_OF_RANGE", "The requested offset is not within the range of offsets maintained by the server.")
var ErrCorruptMessage = NewKafkaException(2, "CORRUPT_MESSAGE", "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.")
var ErrUnknownTopicOrPartition = NewKafkaException(3, "UNKNOWN_TOPIC_OR_PARTITION", "This server does not host this topic-partition.")
var ErrInvalidFetchSize = NewKafkaException(4, "INVALID_FETCH_SIZE", "The requested fetch size is invalid.")
var ErrLeaderNotAvailable = NewKafkaException(5, "LEADER_NOT_AVAILABLE", "There is no leader for this topic-partition as we are in the middle of a leadership election.")
var ErrNotLeaderForPartition = NewKafkaException(6, "NOT_LEADER_FOR_PARTITION", "This server is not the leader for that topic-partition.")
var ErrRequestTimedOut = NewKafkaException(7, "REQUEST_TIMED_OUT", "The request timed out.")
var ErrBrokerNotAvailable = NewKafkaException(8, "BROKER_NOT_AVAILABLE", "The broker is not available.")
var ErrReplicaNotAvailable = NewKafkaException(9, "REPLICA_NOT_AVAILABLE", "The replica is not available for the requested topic-partition.")
var ErrMessageTooLarge = NewKafkaException(10, "MESSAGE_TOO_LARGE", "The request included a message larger than the max message size the server will accept.")
var ErrStaleControllerEpoch = NewKafkaException(11, "STALE_CONTROLLER_EPOCH", "The controller moved to another broker")
var ErrOffsetMetadataTooLarge = NewKafkaException(12, "OFFSET_METADATA_TOO_LARGE", "The metadata field of the offset request was too large.")
var ErrNetworkException = NewKafkaException(13, "NETWORK_EXCEPTION", "The server disconnected before a response was received.")
var ErrCoordinatorLoadInProgress = NewKafkaException(14, "COORDINATOR_LOAD_IN_PROGRESS", "The coordinator is loading and hence can't process requests.")
var ErrCoordinatorNotAvailable = NewKafkaException(15, "COORDINATOR_NOT_AVAILABLE", "The coordinator is not available.")
var ErrNotCoordinator = NewKafkaException(16, "NOT_COORDINATOR", "This is not the correct coordinator.")
var ErrInvalidTopicException = NewKafkaException(17, "INVALID_TOPIC_EXCEPTION", "The request attempted to perform an operation on an invalid topic.")
var ErrRecordListTooLarge = NewKafkaException(18, "RECORD_LIST_TOO_LARGE", "The request included message batch larger than the configured segment size on the server.")
var ErrNotEnoughReplicas = NewKafkaException(19, "NOT_ENOUGH_REPLICAS", "Messages are rejected since there are fewer in-sync replicas than required.")
var ErrNotEnoughReplicasAfterAppend = NewKafkaException(20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", "Messages are written to the log, but to fewer in-sync replicas than required.")
var ErrInvalidRequiredAcks = NewKafkaException(21, "INVALID_REQUIRED_ACKS", "Produce request specified an invalid value for required acks.")
var ErrIllegalGeneration = NewKafkaException(22, "ILLEGAL_GENERATION", "Specified group generation id is not valid.")
var ErrInconsistentGroupProtocol = NewKafkaException(23, "INCONSISTENT_GROUP_PROTOCOL", "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.")
var ErrInvalidGroupID = NewKafkaException(24, "INVALID_GROUP_ID", "The configured groupId is invalid.")
var ErrUnknownMemberID = NewKafkaException(25, "UNKNOWN_MEMBER_ID", "The coordinator is not aware of this member.")
var ErrInvalidSessionTimeout = NewKafkaException(26, "INVALID_SESSION_TIMEOUT", "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).")
var ErrRebalanceInProgress = NewKafkaException(27, "REBALANCE_IN_PROGRESS", "The group is rebalancing, so a rejoin is needed.")
var ErrInvalidCommitOffsetSize = NewKafkaException(28, "INVALID_COMMIT_OFFSET_SIZE", "The committing offset data size is not valid.")
var ErrTopicAuthorizationFailed = NewKafkaException(29, "TOPIC_AUTHORIZATION_FAILED", "Topic authorization failed.")
var ErrGroupAuthorizationFailed = NewKafkaException(30, "GROUP_AUTHORIZATION_FAILED", "Group authorization failed.")
var ErrClusterAuthorizationFailed = NewKafkaException(31, "CLUSTER_AUTHORIZATION_FAILED", "Cluster authorization failed.")
var ErrInvalidTimestamp = NewKafkaException(32, "INVALID_TIMESTAMP", "The timestamp of the message is out of acceptable range.")
var ErrUnsupportedSASLMechanism = NewKafkaException(33, "UNSUPPORTED_SASL_MECHANISM", "The broker does not support the requested SASL mechanism.")
var ErrIllegalSASLState = NewKafkaException(34, "ILLEGAL_SASL_STATE", "Request is not valid given the current SASL state.")
var UnsupportedVersion = NewKafkaException(35, "UNSUPPORTED_VERSION", "The version of API is not supported.")
var ErrTopicAlreadyExists = NewKafkaException(36, "TOPIC_ALREADY_EXISTS", "Topic with this name already exists.")
var ErrInvalidPartitions = NewKafkaException(37, "INVALID_PARTITIONS", "Number of partitions is below 1.")
var ErrInvalidReplicationFactor = NewKafkaException(38, "INVALID_REPLICATION_FACTOR", "Replication factor is below 1 or larger than the number of available brokers.")
var ErrInvalidReplicaAssignment = NewKafkaException(39, "INVALID_REPLICA_ASSIGNMENT", "Replica assignment is invalid.")
var ErrInvalidConfig = NewKafkaException(40, "INVALID_CONFIG", "Configuration is invalid.")
var ErrNotController = NewKafkaException(41, "NOT_CONTROLLER", "This is not the correct controller for this cluster.")
var ErrInvalidRequest = NewKafkaException(42, "INVALID_REQUEST", "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.")
var ErrUnsupportedMessageFormat = NewKafkaException(43, "UNSUPPORTED_FOR_MESSAGE_FORMAT", "The message format version on the broker does not support the request.")
var ErrPolicyViolation = NewKafkaException(44, "POLICY_VIOLATION", "Request parameters do not satisfy the configured policy.")
var ErrOutOfOrderSequenceNumber = NewKafkaException(45, "OUT_OF_ORDER_SEQUENCE_NUMBER", "The broker received an out of order sequence number.")
var ErrDuplicateSequenceNumber = NewKafkaException(46, "DUPLICATE_SEQUENCE_NUMBER", "The broker received a duplicate sequence number.")
var ErrInvalidProducerEpoch = NewKafkaException(47, "INVALID_PRODUCER_EPOCH", "Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.")
var ErrInvalidTxnState = NewKafkaException(48, "INVALID_TXN_STATE", "The producer attempted a transactional operation in an invalid state.")
var ErrInvalidProducerIDMapping = NewKafkaException(49, "INVALID_PRODUCER_ID_MAPPING", "The producer attempted to use a producer id which is not currently assigned to its transactional id.")
var ErrInvalidTransactionTimeout = NewKafkaException(50, "INVALID_TRANSACTION_TIMEOUT", "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).")
var ErrConcurrentTransactions = NewKafkaException(51, "CONCURRENT_TRANSACTIONS", "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.")
var ErrTransactionCoordinatorFenced = NewKafkaException(52, "TRANSACTION_COORDINATOR_FENCED", "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.")
var ErrTransactionalIDAuthorizationFailed = NewKafkaException(53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", "Transactional Id authorization failed.")
var ErrSecurityDisabled = NewKafkaException(54, "SECURITY_DISABLED", "Security features are disabled.")
var ErrOperationNotAttempted = NewKafkaException(55, "OPERATION_NOT_ATTEMPTED", "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.")
var ErrKafkaStorageError = NewKafkaException(56, "KAFKA_STORAGE_ERROR", "Disk error when trying to access log file on the disk.")
var ErrLogDirNotFound = NewKafkaException(57, "LOG_DIR_NOT_FOUND", "The user-specified log directory is not found in the broker config.")
var ErrSASLAuthenticationFailed = NewKafkaException(58, "SASL_AUTHENTICATION_FAILED", "SASL Authentication failed.")
var ErrUnknownProducer = NewKafkaException(59, "UNKNOWN_PRODUCER_ID", "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.")
var ErrReassignmentInProgress = NewKafkaException(60, "REASSIGNMENT_IN_PROGRESS", "A partition reassignment is in progress.")
var ErrDelegationTokenAuthDisabled = NewKafkaException(61, "DELEGATION_TOKEN_AUTH_DISABLED", "Delegation Token feature is not enabled.")
var ErrDelegationTokenNotFound = NewKafkaException(62, "DELEGATION_TOKEN_NOT_FOUND", "Delegation Token is not found on server.")
var ErrDelegationTokenOwnerMismatch = NewKafkaException(63, "DELEGATION_TOKEN_OWNER_MISMATCH", "Specified Principal is not valid Owner/Renewer.")
var ErrDelegationTokenRequestNotAllowed = NewKafkaException(64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.")
var ErrDelegationTokenAuthorizationFailed = NewKafkaException(65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED", "Delegation Token authorization failed.")
var ErrDelegationTokenExpired = NewKafkaException(66, "DELEGATION_TOKEN_EXPIRED", "Delegation Token is expired.")
var ErrInvalidPrincipalType = NewKafkaException(67, "INVALID_PRINCIPAL_TYPE", "Supplied principalType is not supported.")
var ErrNonEmptyGroup = NewKafkaException(68, "NON_EMPTY_GROUP", "The group is not empty.")
var ErrGroupIDNotFound = NewKafkaException(69, "GROUP_ID_NOT_FOUND", "The group id does not exist.")
var ErrFetchSessionIDNotFound = NewKafkaException(70, "FETCH_SESSION_ID_NOT_FOUND", "The fetch session ID was not found.")
var ErrInvalidFetchSessionEpoch = NewKafkaException(71, "INVALID_FETCH_SESSION_EPOCH", "The fetch session epoch is invalid.")
var ErrListnerNotFound = NewKafkaException(72, "LISTENER_NOT_FOUND", "There is no listener on the leader broker that matches the listener on which metadata request was processed.")
var ErrTopicDeletionDisabled = NewKafkaException(73, "TOPIC_DELETION_DISABLED", "Topic deletion is disabled.")
var ErrFencedLeaderEpoch = NewKafkaException(74, "FENCED_LEADER_EPOCH", "The leader epoch in the request is older than the epoch on the broker.")
var ErrUnknownLeaderEpoch = NewKafkaException(75, "UNKNOWN_LEADER_EPOCH", "The leader epoch in the request is newer than the epoch on the broker.")
var ErrUnsupportedCompressionType = NewKafkaException(76, "UNSUPPORTED_COMPRESSION_TYPE", "The requesting client does not support the compression type of given partition.")
var ErrStaleBrokerEpoch = NewKafkaException(77, "STALE_BROKER_EPOCH", "Broker epoch has changed.")
var ErrOffsetNotAvailable = NewKafkaException(78, "OFFSET_NOT_AVAILABLE", "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.")
var ErrMemberIDRequired = NewKafkaException(79, "MEMBER_ID_REQUIRED", "The group member needs to have a valid member id before actually entering a consumer group.")
var ErrPreferredLeaderNotAvailable = NewKafkaException(80, "PREFERRED_LEADER_NOT_AVAILABLE", "The preferred leader was not available.")
var ErrGroupMaxSizeReached = NewKafkaException(81, "GROUP_MAX_SIZE_REACHED", "The consumer group has reached its max size.")
var ErrFencedInstanceID = NewKafkaException(82, "FENCED_INSTANCE_ID", "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.")
var ErrEligibleLeadersNotAvailable = NewKafkaException(83, "ELIGIBLE_LEADERS_NOT_AVAILABLE", "Eligible topic partition leaders are not available.")
var ErrElectionNotNeeded = NewKafkaException(84, "ELECTION_NOT_NEEDED", "Leader election not needed for topic partition.")
var ErrNoReassignmentInProgress = NewKafkaException(85, "NO_REASSIGNMENT_IN_PROGRESS", "No partition reassignment is in progress.")
var ErrGroupSubscribedToTopic = NewKafkaException(86, "GROUP_SUBSCRIBED_TO_TOPIC", "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.")
var ErrInvalidRecord = NewKafkaException(87, "INVALID_RECORD", "This record has failed the validation on broker and hence will be rejected.")
var ErrUnstableOffsetCommit = NewKafkaException(88, "UNSTABLE_OFFSET_COMMIT", "There are unstable offsets that need to be cleared.")