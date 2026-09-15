package message

import (
	"hash/crc32"
	"strconv"
)

// OutboxShardCount is a fixed logical shard count. Keep it stable after rows
// have been written; changing it requires migrating every stored shard_id.
const OutboxShardCount = 1024

// OutboxShardID returns the stable shard for a conversation. CRC32 is also
// used by migration 000007 so existing and new rows use the same assignment.
func OutboxShardID(conversationID int64) int {
	return int(crc32.ChecksumIEEE([]byte(strconv.FormatInt(conversationID, 10))) % OutboxShardCount)
}

// OutboxShardsForWorker returns the shards owned by one globally assigned
// publisher slot. workerCount and workerID are validated by bootstrap.
func OutboxShardsForWorker(workerID, workerCount int) []int {
	if workerCount <= 0 || workerID < 0 || workerID >= workerCount {
		return nil
	}
	shards := make([]int, 0, (OutboxShardCount+workerCount-1)/workerCount)
	for shardID := workerID; shardID < OutboxShardCount; shardID += workerCount {
		shards = append(shards, shardID)
	}
	return shards
}
