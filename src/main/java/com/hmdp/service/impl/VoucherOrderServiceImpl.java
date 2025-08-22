package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.User;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IShopService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setResultType(Long.class);
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
    }

    private BlockingQueue<VoucherOrder> queue = new ArrayBlockingQueue<VoucherOrder>(1024*1024);

    private static final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        executorService.submit(new voucherOrderTask());
    }

    private class voucherOrderTask implements Runnable{
        @Override
        public void run() {
            while(true){
                //获取订单信息
                try {
                    //VoucherOrder voucherOrder = queue.take();
                    //从消息队列中获取订单信息
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.order", ReadOffset.lastConsumed()));
                    //判断是否获取消息成功
                    if(read == null || read.isEmpty()){
                        //失败 再进行下一次循环
                        continue;
                    }
                    //成功 创建订单
                    MapRecord<String, Object, Object> mapRecord = read.get(0);
                    Map<Object, Object> value = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //ack 确认消息
                    stringRedisTemplate.opsForStream()
                            .acknowledge("stream.order","g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error(e.getMessage());
                    //处理异常消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while(true){
                //获取订单信息
                try {
                    //VoucherOrder voucherOrder = queue.take();
                    //从pendinglist中获取订单信息
                    List<MapRecord<String, Object, Object>> read = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.order", ReadOffset.from("0")));
                    //判断是否获取消息成功
                    if(read == null || read.isEmpty()){
                        //失败 pendlist中的消息全部处理完成
                        break;
                    }
                    //成功 创建订单
                    MapRecord<String, Object, Object> mapRecord = read.get(0);
                    Map<Object, Object> value = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    //ack 确认消息
                    stringRedisTemplate.opsForStream()
                            .acknowledge("stream.order","g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error(e.getMessage());
                    //处理异常消息
                }
            }
        }

    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        //SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        //使用redisson进行优化
        RLock redisLock = redissonClient.getLock("lock:order:" + userId );
        //获取锁
        boolean success = redisLock.tryLock();
        if (!success) {
            log.error("不允许重复下单");
        }

        //事务提交后再释放锁
        // synchronized (userId.toString().intern()) {//对不同的id上不同的锁
        //获取代理对象 防止事务失效
        try {
            proxy.createVoucher(voucherOrder);
        } finally {
            redisLock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    /**
     * 实现秒杀功能
     * @param voucherId
     * @return
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取userid
        Long userId = UserHolder.getUser().getId();
        //获取order id
        long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        //判断结果是否为0
        if (result.intValue() != 0) {
            //不为0 没有购买资格 返回错误信息
            return Result.fail("没有购买资格");
        }
        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //获取订单id
        long id = redisIdWorker.nextId("order:");

        //返回订单id
        return Result.ok(id);
    }

    /*
     public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString());
        //判断结果是否为0
        if (result.intValue() != 0) {
            //不为0 没有购买资格 返回错误信息
            return Result.fail("没有购买资格");
        }
        //为0 把下单信息保存到阻塞队列
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id
        voucherOrder.setVoucherId(redisIdWorker.nextId("voucherOrder"));
        //用户id
        voucherOrder.setUserId(userId);
        //优惠券id
        voucherOrder.setVoucherId(voucherId);
        //加入阻塞队列
        queue.add(voucherOrder);

        //获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //获取订单id
        long id = redisIdWorker.nextId("order:");

        //返回订单id
        return Result.ok(id);
    }
     */

    //@Override
    //public Result seckillVoucher(Long voucherId) {
        //查询优惠券
    //SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
    ////判断秒杀是否开始
    //if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
    //    return Result.fail("秒杀未开始");
    //}
    ////判断是否结束
    //if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
    //    return Result.fail("秒杀已结束");
    //}
    ////判断库存
    //Integer stock = voucher.getStock();
    //if (stock < 1) {
    //    return Result.fail("库存不足");
    //}

    //Long userId = UserHolder.getUser().getId();
    ////创建锁对象
    ////SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
    ////使用redisson进行优化
    //RLock redisLock = redissonClient.getLock("lock:order:" + userId );
    ////获取锁
    //boolean success = redisLock.tryLock();
    //if (!success) {
    //    return Result.fail("不允许重复下单");
    //}

    ////事务提交后再释放锁
    //// synchronized (userId.toString().intern()) {//对不同的id上不同的锁
    ////获取代理对象 防止事务失效
    //try {
    //    IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
    //    return proxy.createVoucher(voucherId);
    //} catch (IllegalStateException e) {
    //    throw new RuntimeException(e);
    //} finally {
    //    redisLock.unlock();//释放锁
    //}
    //// }
    //
    /**
     * 通过悲观锁实现一人一单
     * @param voucherOrder
     * @return
     */
    @Transactional
    public Result createVoucher(VoucherOrder voucherOrder) {
        //获取优惠券id
        Long voucherId = voucherOrder.getVoucherId();
        //一人一单
        Long userId = UserHolder.getUser().getId();
        //判断用户是否下过单
        Integer count = query().eq("voucher_id", voucherId).eq("user_id", userId).count();
        if (count > 0) {
            return Result.fail("已下过单");
        }

        //扣减库存
        boolean update = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)//乐观锁 只要库存大于0 防止超卖
                .update();
        if (!update) {
            return Result.fail("扣减库存失败");
        }
        //写入数据库
        save(voucherOrder);
        //返回订单id
        return Result.ok(voucherOrder.getVoucherId());
    }


}
