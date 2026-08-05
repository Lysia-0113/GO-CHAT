-- token_bucket.lua
-- 分布式令牌桶：单次 EVAL 内读取并更新 tokens/last_refill_ms（GOCHAT_REDIS.md §8.2）
-- KEYS[1] = bucket hash key
-- ARGV[1] = 容量 capacity
-- ARGV[2] = 补充速率 per_second（小数）
-- ARGV[3] = 本次消耗 1
-- ARGV[4] = TTL 秒
-- 返回：1=允许；0=拒绝
local tokens_str = redis.call('HGET', KEYS[1], 'tokens')
local refill_str = redis.call('HGET', KEYS[1], 'last_refill_ms')
local now_t = redis.call('TIME')
local now_ms = now_t[1] * 1000 + math.floor(now_t[2] / 1000)
local tokens, last_refill
if not tokens_str then
    tokens = tonumber(ARGV[1])
    last_refill = now_ms
else
    tokens = tonumber(tokens_str)
    last_refill = tonumber(refill_str)
end

local elapsed_ms = now_ms - last_refill
if elapsed_ms < 0 then
    elapsed_ms = 0
end
tokens = tokens + (tonumber(ARGV[2]) * elapsed_ms / 1000)
if tokens > tonumber(ARGV[1]) then
    tokens = tonumber(ARGV[1])
end

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_refill_ms', now_ms)
    redis.call('EXPIRE', KEYS[1], ARGV[4])
    return 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_refill_ms', now_ms)
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 0
