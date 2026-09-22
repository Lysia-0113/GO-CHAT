package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/mysql/model"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// newMockOutboxRepo 构造 sqlmock + GORM 仓储。
func newMockOutboxRepo(t *testing.T) (*OutboxRepository, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	gormDB, err := gorm.Open(mysql.New(mysql.Config{Conn: db, SkipInitializeWithVersion: true}),
		&gorm.Config{SkipDefaultTransaction: true})
	if err != nil {
		t.Fatalf("gorm open: %v", err)
	}
	return NewOutboxRepository(gormDB), mock
}

const outboxPayload = `{"message_id":100,"seq":5,"sender_id":1,"client_msg_id":"c9c9c9c9-0000-0000-0000-000000000001","conversation_id":10,"message_type":1,"content":{"text":"hi"},"created_at":"2026-08-17T00:00:00Z"}`

func TestClaimLocksConversationHeadAndSetsLease(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	shardID := message.OutboxShardID(10)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*FROM message_outbox AS o.*NOT EXISTS.*CAST.*\\$\\.seq.*GROUP BY.*ORDER BY.*LIMIT \\?").
		WillReturnRows(sqlmock.NewRows([]string{"shard_id", "conversation_id"}).AddRow(shardID, int64(10)))
	mock.ExpectQuery("SELECT.*FROM.*conversations.*FOR UPDATE SKIP LOCKED").
		WithArgs(int64(10), 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(10)))
	mock.ExpectQuery("SELECT.*FROM.*message_outbox.*shard_id = \\?.*CAST.*status IN.*ORDER BY CAST.*LIMIT \\?.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"message_id", "event_type", "shard_id", "payload", "status", "retry_count", "next_retry_at", "last_error"}).
			AddRow(int64(100), int64(1), shardID, outboxPayload, int64(model.OutboxPending), int64(0), time.Now().Add(-time.Second), ""))
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*next_retry_at.*WHERE.*message_id = \\?.*event_type = \\?").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	records, err := repo.Claim(context.Background(), 10, []int{shardID}, "slot-1-run-a", 5*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(records) != 1 || records[0].MessageID != 100 || records[0].EventType != model.OutboxEventPush {
		t.Fatalf("unexpected records: %+v", records)
	}
	if records[0].Status != model.OutboxRetrying || records[0].ConversationID != 10 || records[0].Payload.Seq != 5 {
		t.Fatalf("unexpected claimed head: %+v", records[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkPublishedOwned(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*published_at.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?").
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, err := repo.MarkPublished(context.Background(), 100, model.OutboxEventPush, "slot-1-run-a")
	if err != nil || !applied {
		t.Fatalf("mark published: applied=%v err=%v", applied, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkPublishedOwnershipLost(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	mock.ExpectExec("UPDATE.*message_outbox.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?").
		WillReturnResult(sqlmock.NewResult(0, 0))

	applied, err := repo.MarkPublished(context.Background(), 100, model.OutboxEventPush, "stale-owner")
	if err != nil || applied {
		t.Fatalf("expected stale owner no-op, got applied=%v err=%v", applied, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkFailedSchedulesRetryBeforeLimit(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	mock.ExpectQuery("SELECT.*retry_count.*FROM.*message_outbox.*WHERE.*locked_by = \\?.*").
		WillReturnRows(sqlmock.NewRows([]string{"retry_count"}).AddRow(int64(0)))
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*next_retry_at.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?").
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, needsDLQ, err := repo.MarkFailed(context.Background(), 100, model.OutboxEventPush,
		"timeout", 10, 2*time.Second, 5*time.Second, "slot-1-run-a")
	if err != nil || !applied || needsDLQ {
		t.Fatalf("expected retry state, got applied=%v needsDLQ=%v err=%v", applied, needsDLQ, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkFailedAtLimitQueuesDLQAndKeepsOwner(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	mock.ExpectQuery("SELECT.*retry_count.*FROM.*message_outbox.*WHERE.*locked_by = \\?.*").
		WillReturnRows(sqlmock.NewRows([]string{"retry_count"}).AddRow(int64(9)))
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*next_retry_at.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?").
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, needsDLQ, err := repo.MarkFailed(context.Background(), 100, model.OutboxEventPush,
		"timeout", 10, 2*time.Second, 5*time.Second, "slot-1-run-a")
	if err != nil || !applied || !needsDLQ {
		t.Fatalf("expected DLQ pending state, got applied=%v needsDLQ=%v err=%v", applied, needsDLQ, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestMarkFailedOwnershipLost(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	mock.ExpectQuery("SELECT.*retry_count.*FROM.*message_outbox.*WHERE.*locked_by = \\?.*").
		WillReturnRows(sqlmock.NewRows([]string{"retry_count"}))

	applied, needsDLQ, err := repo.MarkFailed(context.Background(), 100, model.OutboxEventPush,
		"timeout", 10, 2*time.Second, 5*time.Second, "stale-owner")
	if err != nil || applied || needsDLQ {
		t.Fatalf("expected lost ownership no-op, got applied=%v needsDLQ=%v err=%v", applied, needsDLQ, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDLQRetryAndDeadTransitionsAreFenced(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*next_retry_at.*WHERE.*status = \\?.*locked_by = \\?").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if applied, err := repo.RetryDLQ(context.Background(), 100, model.OutboxEventPush, time.Second, "slot-1-run-a"); err != nil || !applied {
		t.Fatalf("retry dlq: applied=%v err=%v", applied, err)
	}
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*status.*WHERE.*status = \\?.*locked_by = \\?").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if applied, err := repo.MarkDead(context.Background(), 100, model.OutboxEventPush, "slot-1-run-a"); err != nil || !applied {
		t.Fatalf("mark dead: applied=%v err=%v", applied, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
