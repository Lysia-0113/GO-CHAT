package message

import "testing"

func TestOutboxShardIDIsStable(t *testing.T) {
	if got := OutboxShardID(10); got != 481 {
		t.Fatalf("OutboxShardID(10) = %d, want stable shard 481", got)
	}
	if got := OutboxShardID(0); got < 0 || got >= OutboxShardCount {
		t.Fatalf("shard out of range: %d", got)
	}
}

func TestOutboxShardsForWorkerCoversEveryShardOnce(t *testing.T) {
	const workers = 7
	seen := make([]int, OutboxShardCount)
	for workerID := 0; workerID < workers; workerID++ {
		for _, shardID := range OutboxShardsForWorker(workerID, workers) {
			seen[shardID]++
		}
	}
	for shardID, count := range seen {
		if count != 1 {
			t.Fatalf("shard %d assigned %d times; want exactly once", shardID, count)
		}
	}
}
