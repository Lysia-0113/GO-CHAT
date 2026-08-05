-- release_message_idem.lua
-- 条件更新幂等状态：仅当值等于自己的 nonce 时才修改（GOCHAT_REDIS.md §7.2 第 5/6 步）
-- KEYS[1] = idem key
-- ARGV[1] = 自己的 nonce 前缀（"processing:"）
-- ARGV[2] = 目标值（accepted 或空串表示删除）
-- ARGV[3] = accepted TTL 毫秒（仅当目标为 accepted 时生效）
local value = redis.call('GET', KEYS[1])
if not value or value ~= ARGV[1] then
    return 0
end
if ARGV[2] == '' then
    redis.call('DEL', KEYS[1])
else
    redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
end
return 1
