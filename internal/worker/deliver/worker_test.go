package deliver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/Lysia-0113/GO-CHAT/internal/connection"
	"github.com/Lysia-0113/GO-CHAT/internal/infrastructure/kafka"
	"github.com/Lysia-0113/GO-CHAT/internal/message"
	"github.com/Lysia-0113/GO-CHAT/internal/svc"
)

type failedPushConn struct {
	id     string
	userID int64
}

func (c *failedPushConn) ID() string       { return c.id }
func (c *failedPushConn) UserID() int64    { return c.userID }
func (c *failedPushConn) DeviceID() string { return "test-device" }
func (c *failedPushConn) Push(context.Context, connection.Event) error {
	return errors.New("queue full")
}
func (c *failedPushConn) Close(string) {}
func (c *failedPushConn) Closed() bool { return false }

func newPushFailureWorker(t *testing.T) (*Worker, kafka.Message) {
	t.Helper()
	connManager := connection.NewManager("node-1", nil)
	if err := connManager.Register(context.Background(), &failedPushConn{id: "conn-2", userID: 2}); err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svcCtx := &svc.ServiceContext{ConnManager: connManager, Log: logger}
	worker := &Worker{
		svcCtx: svcCtx,
		dedup:  newDedupe(time.Minute),
		commitMessage: func(context.Context, kafka.Message) error {
			return nil
		},
		publishDLQ: func(context.Context, kafka.Envelope) error {
			return nil
		},
	}
	event := message.MessagePersistedEvent{
		MessageID:       99,
		Seq:             7,
		SenderID:        1,
		ClientMessageID: "client-message-99",
		ConversationID:  123,
		MessageType:     message.TypeText,
		Content:         json.RawMessage(`{"text":"hello"}`),
		CreatedAt:       time.Now().UTC(),
	}
	pushEvent := message.MessagePushEvent{
		MessageID:       event.MessageID,
		Seq:             event.Seq,
		SenderID:        event.SenderID,
		ClientMessageID: event.ClientMessageID,
		ConversationID:  event.ConversationID,
		MessageType:     event.MessageType,
		Content:         event.Content,
		CreatedAt:       event.CreatedAt,
		TargetUserIDs:   []int64{2},
	}
	env, err := kafka.NewEnvelope(kafka.EventPush, "test", event.ConversationID, pushEvent)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return worker, kafka.Message{Topic: "im.message.push.dev", Partition: 2, Offset: 44, Value: raw}
}

func TestHandleWritesPushFailureToDLQBeforeOffsetCommit(t *testing.T) {
	w, msg := newPushFailureWorker(t)
	var steps []string
	w.publishDLQ = func(_ context.Context, env kafka.Envelope) error {
		steps = append(steps, "dlq")
		var payload kafka.DLQPayload
		if err := json.Unmarshal(env.Data, &payload); err != nil {
			t.Fatalf("decode dlq payload: %v", err)
		}
		if payload.FailedTopic != msg.Topic || payload.FailedPartition != msg.Partition || payload.FailedOffset != msg.Offset {
			t.Fatalf("wrong failed Kafka position in DLQ: %+v", payload)
		}
		if payload.ErrorCode != "WEBSOCKET_PUSH_FAILED" || payload.RetryCount != 0 {
			t.Fatalf("unexpected DLQ classification: %+v", payload)
		}
		return nil
	}
	w.commitMessage = func(_ context.Context, got kafka.Message) error {
		steps = append(steps, "commit")
		if got.Offset != msg.Offset {
			t.Fatalf("committed wrong offset: %d", got.Offset)
		}
		return nil
	}

	if err := w.handle(context.Background(), msg); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if !reflect.DeepEqual(steps, []string{"dlq", "commit"}) {
		t.Fatalf("expected DLQ write before commit, got %v", steps)
	}
	if !w.dedup.Seen(99) {
		t.Fatal("successful DLQ write should mark the event handled")
	}
}

func TestHandleDoesNotCommitWhenPushFailureDLQWriteFails(t *testing.T) {
	w, msg := newPushFailureWorker(t)
	committed := false
	w.publishDLQ = func(context.Context, kafka.Envelope) error {
		return errors.New("DLQ unavailable")
	}
	w.commitMessage = func(context.Context, kafka.Message) error {
		committed = true
		return nil
	}

	if err := w.handle(context.Background(), msg); err == nil {
		t.Fatal("expected DLQ failure to be returned")
	}
	if committed {
		t.Fatal("offset must remain uncommitted when DLQ write fails")
	}
	if w.dedup.Seen(99) {
		t.Fatal("failed DLQ write must not mark the event handled")
	}
}
