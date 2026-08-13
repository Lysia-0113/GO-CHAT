-- advance_cursor.lua
-- 会话同步游标只增不减（GOCHAT_REDIS.md §10）：并发推进/响应乱序时保持最大值。
-- KEYS[1] = cursor hash (im:cursor:{conversation_id})
-- ARGV[1] = user_id (field)
-- ARGV[2] = new seq
local cur = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
local new = tonumber(ARGV[2])
if new > cur then
    redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
    return new
end
return cur
