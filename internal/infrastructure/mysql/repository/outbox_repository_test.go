package repository

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// newMockOutboxRepo 构造 sqlmock + GORM 仓储。
// SkipDefaultTransaction：GORM 默认给写操作自动包事务，测试里显式控制 Begin/Commit。
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

// TestClaimSetsLease 回归用例：领取时必须把 next_retry_at 一起写入 SET 子句。
// 这正是"重复发布" bug 的根源——漏写 next_retry_at 时，
// 行在发布期间仍满足 `next_retry_at <= now`，会被其他 Publisher 再次领取。
func TestClaimSetsLease(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT.*message_outbox.*FOR UPDATE SKIP LOCKED").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"message_id", "event_type", "payload"}).
			AddRow(int64(100), int64(1), outboxPayload))
	// SET：locked_by/status/locked_at/next_retry_at + GORM 自动附带 updated_at = 5 列。
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*next_retry_at.*WHERE.*message_id = \\?.*event_type = \\?.*").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	records, err := repo.Claim(context.Background(), 10, "node-1", 5*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(records) != 1 || records[0].MessageID != 100 || records[0].EventType != 1 {
		t.Fatalf("unexpected records: %+v", records)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestMarkPublishedOwned：持有者匹配时标记成功，返回 applied=true。
func TestMarkPublishedOwned(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)

	// SET：status/published_at/locked_by/locked_at/last_error + updated_at = 6 列。
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*published_at.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?.*").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, err := repo.MarkPublished(context.Background(), 100, 1, "node-1")
	if err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if !applied {
		t.Fatal("expected applied=true when ownership matches")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestMarkPublishedOwnershipLost：行已被其他 Publisher 重新领取（locked_by 不匹配）
// 时更新 0 行，返回 applied=false 且不报错——过期持有者不得覆盖新状态。
func TestMarkPublishedOwnershipLost(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)

	mock.ExpectExec("UPDATE.*message_outbox.*SET.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?.*").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	applied, err := repo.MarkPublished(context.Background(), 100, 1, "node-1")
	if err != nil {
		t.Fatalf("mark published: %v", err)
	}
	if applied {
		t.Fatal("expected applied=false when ownership lost")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestMarkFailedOwned：正常失败路径——读 retry_count 后更新状态，返回 applied=true。
func TestMarkFailedOwned(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)

	// SELECT 带 LIMIT ? 参数，共 4 个参数。
	mock.ExpectQuery("SELECT.*retry_count.*FROM.*message_outbox.*WHERE.*locked_by = \\?.*").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"retry_count"}).AddRow(int64(0)))
	// SET：status/retry_count/next_retry_at/last_error/locked_by/locked_at + updated_at = 7 列。
	mock.ExpectExec("UPDATE.*message_outbox.*SET.*next_retry_at.*WHERE.*message_id = \\?.*event_type = \\?.*locked_by = \\?.*").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	applied, err := repo.MarkFailed(context.Background(), 100, 1, "timeout", 10, 2*time.Second, "node-1")
	if err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if !applied {
		t.Fatal("expected applied=true when ownership matches")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestMarkFailedOwnershipLost：所有权已转移（租约过期被重新领取）时读取不到行，
// 返回 applied=false 且不报错——过期持有者不得推进状态。
func TestMarkFailedOwnershipLost(t *testing.T) {
	repo, mock := newMockOutboxRepo(t)

	mock.ExpectQuery("SELECT.*retry_count.*FROM.*message_outbox.*WHERE.*locked_by = \\?.*").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"retry_count"}))

	applied, err := repo.MarkFailed(context.Background(), 100, 1, "timeout", 10, 2*time.Second, "node-1")
	if err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if applied {
		t.Fatal("expected applied=false when ownership lost")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
