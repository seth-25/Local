package com.cherry;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Insert and Search Lock
 */
public class SearchLock {
    public int searchNum = 0;

    public Lock lock;
    public Condition condition;

    public SearchLock() {
        this.lock = new ReentrantLock();
        this.condition = lock.newCondition();
    }
}
