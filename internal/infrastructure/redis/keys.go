// Package redis 是 Redis 基础设施：同步游标、Presence、票据、幂等、限流。
package redis

import (
	"fmt"
	"strconv"
)

// Key 约定：im:{领域}:{业务标识}:{用途}（GOCHAT_REDIS.md §2.2）。

// CursorKey 会话同步游标 HASH：field=user_id，value=该用户已同步到的最新 seq。
// 两层游标设计（GOCHAT_REDIS.md §10）：服务端记录，查询基准始终是客户端本地游标。
func CursorKey(conversationID int64) string {
	return fmt.Sprintf("im:cursor:%d", conversationID)
}

// PresenceUserKey 用户在线连接 ZSET：member=connection_id，score=过期毫秒时间戳。
func PresenceUserKey(userID int64) string {
	return fmt.Sprintf("im:presence:%d:conn", userID)
}

// PresenceConnKey 连接路由 HASH：user_id/device_id/node_id/connected_at。
func PresenceConnKey(connectionID string) string {
	return fmt.Sprintf("im:presence:conn:%s", connectionID)
}

// WSTicketKey 一次性 WebSocket 票据 STRING：JSON{user_id, device_id, issued_at}。
func WSTicketKey(token string) string {
	return fmt.Sprintf("im:ws-ticket:%s", token)
}

// IdemKey 消息入口快速幂等 STRING：processing:{nonce} / accepted。
func IdemKey(senderID int64, clientMessageID string) string {
	return fmt.Sprintf("im:idem:%d:%s", senderID, clientMessageID)
}

// RateSendKey 用户发送令牌桶 HASH：tokens/last_refill_ms。
func RateSendKey(userID int64) string {
	return fmt.Sprintf("im:rate:send:%d", userID)
}

// RateSendConvKey 会话发送令牌桶 HASH。
func RateSendConvKey(conversationID int64) string {
	return fmt.Sprintf("im:rate:send:conv:%d", conversationID)
}

// RateHistoryKey 历史查询令牌桶 HASH。
func RateHistoryKey(userID, conversationID int64) string {
	return fmt.Sprintf("im:rate:history:%d:%d", userID, conversationID)
}

// RateLoginKey 登录限流 HASH。
func RateLoginKey(ip, username string) string {
	return fmt.Sprintf("im:rate:login:%s:%s", ip, username)
}

// RateWSConnectIPKey WebSocket 建连限流（IP 维度）。
func RateWSConnectIPKey(ip string) string {
	return fmt.Sprintf("im:rate:ws:ip:%s", ip)
}

// RateWSConnectUserKey WebSocket 建连限流（用户维度）。
func RateWSConnectUserKey(userID int64) string {
	return fmt.Sprintf("im:rate:ws:user:%d", userID)
}

// GatewayChannel 按 node_id 划分的 Pub/Sub 投递通道（GOCHAT_REDIS.md §9.1）。
func GatewayChannel(nodeID string) string {
	return fmt.Sprintf("im:gateway:%s", nodeID)
}

// IDString 将 int64 转为十进制字符串（供 Redis JSON 快照使用）。
func IDString(id int64) string { return strconv.FormatInt(id, 10) }
