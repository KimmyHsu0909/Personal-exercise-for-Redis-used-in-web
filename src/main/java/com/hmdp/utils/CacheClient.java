package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {

    private StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value,Long time, TimeUnit timeUnit){
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(value),time,timeUnit);
    }

    public void setWithLogicalExpire(String key, Object value,Long time, TimeUnit timeUnit) {
        //设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));
        //存入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID>R queryWithPassThrough(
            String prefixkey, ID id, Class<R> clazz, Function<ID,R> dbFallback, Long time, TimeUnit timeUnit) {
        String key = prefixkey + id;
        //从redis查询
        String json = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
        //判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //若存在 返回
            R r = JSONUtil.toBean(json, clazz);
            return r;
        }
        //判断是否为空值
        if (json == null) {
            return null;
        }
        //不存在查询数据库
        R r = dbFallback.apply(id);
        //数据库不存在 返回错误
        if (r == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", time, timeUnit);
            //返回错误信息
            return null;
        }
        //存在 写入redis
        setWithLogicalExpire(key, r, time, timeUnit);
        //返回
        return r;
    }

    private final ExecutorService executorService = Executors.newFixedThreadPool(10) ;

    public <R,ID> R queryWithLogicalExpire(String prefixkey, ID id, Class<R> clazz, Function<ID,R> dbFallback, Long time, TimeUnit timeUnit) {
        String key = prefixkey + id;
        //从redis查询
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isBlank(json)) {
            //若不存在 返回null
            return null;
        }
        //命中 把json序列化 从json中获取过期时间
        RedisData data = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) data.getData(), clazz);
        LocalDateTime expireTime = data.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期 直接返回信息
            return r;
        }
        //已经过期 缓存重建
        //缓存重建 获取互斥锁
        Boolean lock = trylock(key);
        //判断是否获取锁
        if (lock) {
            //成功 开启新线程 进行缓存重建
            executorService.submit(() -> {
                try {
                    //缓存重建
                    //查询数据库
                    R apply = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicalExpire(key, apply, time, timeUnit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(key);
                }
            });

        }
        //失败 返回旧信息
        return r;
    }

    private Boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

}
