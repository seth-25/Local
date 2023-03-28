package com.cherry.domain;

public class Pair<K, V> {
    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }
    private final K key;
    private final V value;
    public K getKey() { return key; }
    public V getValue() { return value; }
}
