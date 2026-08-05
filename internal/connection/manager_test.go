package connection

import (
	"context"
	"encoding/json"
	"testing"
)

// fakeConn 是测试用连接。
type fakeConn struct {
	id     string
	userID int64
	device string
	events []Event
	closed bool
}

func (f *fakeConn) ID() string       { return f.id }
func (f *fakeConn) UserID() int64    { return f.userID }
func (f *fakeConn) DeviceID() string { return f.device }
func (f *fakeConn) Push(ctx context.Context, e Event) error {
	f.events = append(f.events, e)
	return nil
}
func (f *fakeConn) Close(reason string) { f.closed = true }
func (f *fakeConn) Closed() bool        { return f.closed }

func TestManagerRegisterUnregister(t *testing.T) {
	m := NewManager("node-1", nil)
	conn := &fakeConn{id: "conn-1", userID: 10}
	if err := m.Register(context.Background(), conn); err != nil {
		t.Fatal(err)
	}
	if m.Count() != 1 {
		t.Fatalf("expected 1 connection, got %d", m.Count())
	}
	if err := m.PushToConnection(context.Background(), "conn-1", Event{Event: "test"}); err != nil {
		t.Fatal(err)
	}
	if len(conn.events) != 1 {
		t.Fatalf("expected 1 pushed event, got %d", len(conn.events))
	}
	m.Unregister(context.Background(), conn)
	if m.Count() != 0 {
		t.Fatalf("expected 0 connections, got %d", m.Count())
	}
	if err := m.PushToConnection(context.Background(), "conn-1", Event{}); err != ErrConnectionNotFound {
		t.Fatalf("expected ErrConnectionNotFound, got %v", err)
	}
}

func TestManagerPushToUser(t *testing.T) {
	m := NewManager("node-1", nil)
	a := &fakeConn{id: "conn-a", userID: 10}
	b := &fakeConn{id: "conn-b", userID: 10}
	c := &fakeConn{id: "conn-c", userID: 20}
	for _, conn := range []*fakeConn{a, b, c} {
		if err := m.Register(context.Background(), conn); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := NewEvent(EventMessageNew, map[string]string{"hello": "world"})
	if err != nil {
		t.Fatal(err)
	}
	delivered := m.PushToUser(context.Background(), 10, ev)
	if delivered != 2 {
		t.Fatalf("expected 2 delivered, got %d", delivered)
	}
	if len(a.events) != 1 || len(b.events) != 1 {
		t.Fatal("both user connections must receive")
	}
	if len(c.events) != 0 {
		t.Fatal("other user must not receive")
	}
	// 事件信封格式
	var body struct {
		Event string          `json:"event"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(mustMarshal(a.events[0])), &body); err != nil {
		t.Fatal(err)
	}
	if body.Event != EventMessageNew {
		t.Fatalf("unexpected event: %s", body.Event)
	}
}

func TestManagerUnregisterOneUserKeepsOthers(t *testing.T) {
	m := NewManager("node-1", nil)
	a := &fakeConn{id: "conn-a", userID: 10}
	b := &fakeConn{id: "conn-b", userID: 10}
	if err := m.Register(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err := m.Register(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	m.Unregister(context.Background(), a)
	if delivered := m.PushToUser(context.Background(), 10, Event{Event: "x"}); delivered != 1 {
		t.Fatalf("expected 1 delivered after unregister, got %d", delivered)
	}
}

func mustMarshal(e Event) []byte {
	raw, _ := json.Marshal(e)
	return raw
}
