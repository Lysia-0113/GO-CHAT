-- acquire_message_idem.lua
-- 消息入口快速幂等：SET NX + TTL（GOCHAT_REDIS.md §7.2）
-- KEYS[1] = idem key
-- ARGV[1] = processing:{nonce} 值
-- ARGV[2] = processing TTL 毫秒
-- ARGV[3] = accepted 值
-- 返回：1=获得发送权；2=已 accepted；3=处理中
local value = redis.call('GET', KEYS[1])
if value then
    if value == ARGV[3] then
        return 2
    end
    return 3
end
local ok = redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
if ok then
    return 1
end
return 3
