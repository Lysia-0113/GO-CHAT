package kafka

import (
	"encoding/json"
	"testing"

	"github.com/Lysia-0113/GO-CHAT/internal/message"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	event := message.MessageIngressEvent{
		SenderID:        1,
		ClientMessageID: "019fd1c3-8b25-7ba0-a49d-001122334455",
		ConversationID:  9001,
		MessageType:     message.TypeText,
		Content:         json.RawMessage(`{"text":"你好"}`),
	}
	env, err := NewEnvelope(EventIngress, "ws-gateway", event.ConversationID, event)
	if err != nil {
		t.Fatal(err)
	}
	if env.SchemaVersion != 1 || env.EventType != EventIngress || env.ConversationID != "9001" {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	if env.EventID == "" || env.Producer != "ws-gateway" {
		t.Fatalf("missing envelope fields: %+v", env)
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var back Envelope
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	var payload message.MessageIngressEvent
	if err := json.Unmarshal(back.Data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.SenderID != 1 || payload.ConversationID != 9001 {
		t.Fatalf("payload mismatch: %+v", payload)
	}
}

func TestTopicsSuffix(t *testing.T) {
	plain := NewTopics("")
	if plain.Ingress() != "im.message.ingress" || plain.Persisted() != "im.message.persisted" || plain.DLQ() != "im.message.dlq" {
		t.Fatalf("unexpected topics: %+v", plain)
	}
	dev := NewTopics("dev")
	if dev.Ingress() != "im.message.ingress.dev" {
		t.Fatalf("unexpected suffixed topic: %s", dev.Ingress())
	}
}

func TestKeyOf(t *testing.T) {
	if KeyOf(9001) != "9001" {
		t.Fatalf("unexpected key: %s", KeyOf(9001))
	}
}

func TestDLQPayloadKeepsOriginal(t *testing.T) {
	orig, _ := json.Marshal(Envelope{SchemaVersion: 1, EventType: EventIngress, EventID: "evt_x"})
	payload := DLQPayload{
		FailedTopic:     "im.message.ingress",
		FailedPartition: 3,
		FailedOffset:    10882,
		RetryCount:      5,
		ErrorCode:       "MYSQL_DEADLOCK",
		OriginalEvent:   orig,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var back DLQPayload
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.FailedOffset != 10882 || back.RetryCount != 5 {
		t.Fatalf("DLQ payload mismatch: %+v", back)
	}
}
