package org.apache.flink.formats.protobuf.serialize;

public class Entry<K, V> {
  public Entry(K k, V v) {
    this.key = k;
    this.value = v;
  }

  public K key;

  public V value;

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  public static <K, V> Builder<K, V> newBuilder() {
    return new Builder<>();
  }

  public static class Builder<K, V> {
    private K key;
    private V value;

    public void setKey(K key) {
      this.key = key;
    }

    public void setValue(V value) {
      this.value = value;
    }

    public Entry<K, V> build() {
      return new Entry<>(key, value);
    }
  }
}
