package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ShopMapper shopMapper;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        Function<Long, Shop> dbFallback = n->shopMapper.selectById(n);
        //Shop shop =  cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
        //        dbFallback, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
        //Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class,
        //                dbFallback, 20L, TimeUnit.MINUTES);


        //逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }

    private final ExecutorService executorService = Executors.newFixedThreadPool(10) ;

    /**
     * 逻辑过期时间解决缓存击穿
     * @param id
     * @return
     */

    public Shop queryWithLogicalExpire(Long id) {
        //从redis查询
        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
        //判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //若不存在 返回null
            return null;
        }
        //命中 把json序列化 从json中获取过期时间
        RedisData data = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) data.getData(), Shop.class);
        LocalDateTime expireTime = data.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期 直接返回信息
            return shop;
        }
        //已经过期 缓存重建
        //缓存重建 获取互斥锁
        Boolean lock = trylock("lock:shop:" + id);
        //判断是否获取锁
        if (lock) {
            //成功 开启新线程 进行缓存重建
            executorService.submit(() -> {
                try {
                    //缓存重建
                    this.saveShop2Redis(id, 30);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock("lock:shop:" + id);
                }
            });

        }
        //失败 返回旧信息
        return shop;
    }

    /**
     * 互斥锁解决缓存击穿
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        //从redis查询
        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //若存在 返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否为空值
        if (shopJson == null) {
            return null;
        }

        //新增：尝试获取互斥锁
        String key = "key:shop:" + id;
        Boolean lock = trylock(key);
        Shop shop = null;
        try {
            //判断是否得到
            if (!lock) {
                //未得到互斥锁，进行休眠
                Thread.sleep(10);
                return queryWithMutex(id);
            }

            //得到 进行数据库操作

            //不存在查询数据库
            shop = shopMapper.selectById(id);
            //数据库不存在 返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set("cache:shop:" + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;
            }
            //存在 写入redis
            stringRedisTemplate.opsForValue().set("cache:shop:" + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放互斥锁
            unlock(key);
        }
        //返回
        return shop;
    }

    /**
     * 解决缓存穿透
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        //从redis查询
        String shopJson = stringRedisTemplate.opsForValue().get("cache:shop:" + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //若存在 返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断是否为空值
        if (shopJson == null) {
            return null;
        }
        //不存在查询数据库
        Shop shop = shopMapper.selectById(id);
        //数据库不存在 返回错误
        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set("cache:shop:" + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;
        }
        //存在 写入redis
        stringRedisTemplate.opsForValue().set("cache:shop:" + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }

    /**
     * 获取锁
     * @param key
     * @return
     */
    private Boolean trylock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 封装逻辑过期时间并存入redis
     * @param id
     * @param expireTime
     */
    private void saveShop2Redis(Long id, int expireTime) {
        Shop shop = shopMapper.selectById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(expireTime));
        //存入redis
        stringRedisTemplate.opsForValue().set("cache:shop" + id,JSONUtil.toJsonStr(redisData));
    }

    /**
     *更新店铺
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            Result.fail("id 不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete("cache:shop:" + shop.getId());
        return Result.ok();
    }
}
