package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleRedisLock implements Ilock {

    public SimpleRedisLock(String name, StringRedisTemplate redisTemplate) {
        this.name = name;
        this.redisTemplate = redisTemplate;
    }

    private String name;//业务名
    private StringRedisTemplate redisTemplate;
    private static final String PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    //初始化脚本 减少io流
    private static final DefaultRedisScript<Long> DEFAULT_SCRIPT;
    static {
        DEFAULT_SCRIPT = new DefaultRedisScript<>();
        DEFAULT_SCRIPT.setResultType(Long.class);
        DEFAULT_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
    }

    @Override
    public boolean trylock(int timeOutSec) {
        //获取当前线程 加上uuid区分锁 防止误删
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        String key = PREFIX + name;
        //获取锁
        Boolean success = redisTemplate.opsForValue()
                .setIfAbsent(key, threadId, timeOutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);//防止拆箱时nullpointer
    }

    /**
     * 释放锁
     */
    @Override
    public void unlock() {
        //判断锁是否相同
        //String currentThread = ID_PREFIX + Thread.currentThread().getId();
        //String threadId = redisTemplate.opsForValue().get(PREFIX + name);
        //if (Objects.equals(threadId, currentThread)) {
        //    redisTemplate.delete(PREFIX + name);
        //}

        //使用lua脚本 满足原子性
        redisTemplate.execute(DEFAULT_SCRIPT,
                Collections.singletonList(PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }
}
