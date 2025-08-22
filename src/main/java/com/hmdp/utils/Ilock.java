package com.hmdp.utils;

public interface Ilock {

    /**
     * 获取锁
     * @param timeOutSec
     * @return
     */
    boolean trylock(int timeOutSec) ;

    /**
     * 释放锁
     */
    void unlock() ;
}
