-- append_recent_message.lua
-- 原子追加最近消息：ZSET 索引 + HASH 快照 + 超量裁剪 + 配对 TTL（GOCHAT_REDIS.md §4.3）
-- KEYS[1] = idx zset
-- KEYS[2] = data hash
-- ARGV[1] = seq (score)
-- ARGV[2] = message_id (member / field)
-- ARGV[3] = 消息 JSON 快照
-- ARGV[4] = max_messages
-- ARGV[5] = ttl 秒
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])

local total = redis.call('ZCARD', KEYS[1])
local overflow = total - tonumber(ARGV[4])
if overflow > 0 then
    local expired = redis.call('ZRANGE', KEYS[1], 0, overflow - 1)
    if #expired > 0 then
        redis.call('ZREM', KEYS[1], unpack(expired))
        redis.call('HDEL', KEYS[2], unpack(expired))
    end
end

redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])
return 1
