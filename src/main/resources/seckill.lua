-- 参数列表
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

-- key列表
-- 库存
local stockKey = "seckill:stock:"..voucher
--订单
local orderKey = "seckill:order:"..voucher

-- 脚本业务
-- 判断库存是否充足
if (tonumber(redis.call("get", stockKey)) <= 0) then
    return 1
end
-- 判断用户是否下单
if (redis.call("get", orderKey) == 1) then
    return 2
end
--扣减库存
redis.call("incrby", stockKey, -1)
--下单
redis.call("sadd", orderKey, userId)
--发送消息到队列中
redis.call("xadd", "stream.order","*", "voucherId", voucherId, "userId",userId, "id", orderId )
return 0