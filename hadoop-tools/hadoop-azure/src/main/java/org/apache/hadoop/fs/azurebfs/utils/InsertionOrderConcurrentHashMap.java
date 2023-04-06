package org.apache.hadoop.fs.azurebfs.utils;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.azurebfs.services.BlockWithId;

public class InsertionOrderConcurrentHashMap<K, V> {

  private final Map<K, V> map = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<K> queue = new ConcurrentLinkedQueue<K>();

  Comparator<BlockWithId> comparator = (o1, o2) -> (int) (o1.getOffset() - o2.getOffset());

  public V put(K key, V value) {
    V result = map.put(key, value);
    if (result == null) {
      queue.add(key);
    }
    return result;
  }

  public V get(Object key) {
    return map.get(key);
  }

  public V remove(Object key) {
    V result = map.remove(key);
    if (result != null) {
      queue.remove(key);
    }
    return result;
  }

  public K peek() {
    return queue.peek();
  }

  public K poll() {
    K key = queue.poll();
    if (key != null) {
      map.remove(key);
    }
    return key;
  }

  public int size() {
    return map.size();
  }

  public ConcurrentLinkedQueue<K> getQueue() {
    ConcurrentLinkedQueue<K> sortedQueue = queue.stream()
        .sorted((Comparator<? super K>) comparator)
        .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
    return sortedQueue;
  }

  public boolean containsKey(K key) {
    for (Map.Entry<K, V> entry : entrySet()) {
      if (entry.getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  public Set<Map.Entry<K, V>> entrySet() {
    return new HashSet<>(map.entrySet());
  }

  public void clear() {
    map.clear();
    while (!queue.isEmpty()) {
      queue.poll();
    }
  }
}





