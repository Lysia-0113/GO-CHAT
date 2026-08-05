-- consume_ws_ticket.lua
-- 一次性消费 WebSocket Ticket：读取并删除（GOCHAT_REDIS.md §6.2）
-- KEYS[1] = ticket key
local value = redis.call('GET', KEYS[1])
if not value then
    return nil
end
redis.call('DEL', KEYS[1])
return value
