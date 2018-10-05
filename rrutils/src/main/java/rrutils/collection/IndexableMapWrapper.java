package rrutils.collection;

import org.agrona.BitUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2IntHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.reactivestreams.Publisher;
import org.roaringbitmap.RoaringBitmap;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;

import java.util.*;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

class IndexableMapWrapper<K, V> extends IndexableMap<K, V> {
  public static final long mask = Integer.MAX_VALUE - 1;
  private final Map<K, V> delegate;
  private final DirectProcessor<Event> events;
  private final Int2ObjectHashMap<K> indexToKey;
  private final Object2IntHashMap<K> keyToIndex;
  private final Object2ObjectHashMap<K, Object2ObjectHashMap<String, String>> keyToTags;
  private final Object2ObjectHashMap<String, Object2ObjectHashMap<String, RoaringBitmap>>
      tagIndexes;
  AtomicLongFieldUpdater<IndexableMapWrapper> KEY_INDEX =
      AtomicLongFieldUpdater.newUpdater(IndexableMapWrapper.class, "keyIndex");
  volatile long keyIndex;

  public IndexableMapWrapper(Map<K, V> delegate) {
    this.delegate = delegate;
    this.events = DirectProcessor.create();
    this.tagIndexes = new Object2ObjectHashMap<>();
    this.indexToKey = new Int2ObjectHashMap<>();
    this.keyToTags = new Object2ObjectHashMap<>();
    this.keyToIndex = new Object2IntHashMap<>(-1);
  }

  // -- IndexableMap Methods --
  @Override
  public V put(K key, V value, String... tags) {
    V put = put(key, value);

    try {
      addTags(key, tags);
    } catch (Throwable t) {
      delegate.remove(key);
      throw Exceptions.bubble(t);
    }

    return put;
  }

  @Override
  public void addTags(K key, String... tags) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(tags);

    int length = tags.length;

    int keyIndex;
    synchronized (this) {
      if (!keyToIndex.containsKey(key)) {
        throw new IllegalStateException("missing key");
      }

      if (!BitUtil.isEven(length)) {
        throw new IllegalArgumentException("tags must be even");
      }

      keyIndex = keyToIndex.get(key);

      for (int i = 0; i < length; ) {
        String k = tags[i++];
        String v = tags[i++];

        RoaringBitmap bitmap =
            tagIndexes
                .computeIfAbsent(k, _k -> new Object2ObjectHashMap<>())
                .computeIfAbsent(v, _v -> new RoaringBitmap());

        bitmap.add(keyIndex);

        keyToTags.computeIfAbsent(key, _k -> new Object2ObjectHashMap<>()).put(k, v);
      }
    }

    Event<K> kEvent = Event.of(Event.EventType.ADD_TAG, key, Collections.EMPTY_MAP);
    events.onNext(kEvent);
  }

  @Override
  public void removeTags(K key, String... tags) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(tags);

    int length = tags.length;

    int keyIndex;
    synchronized (this) {
      if (!keyToIndex.containsKey(key)) {
        return;
      }

      keyIndex = keyToIndex.get(key);

      for (int i = 0; i < length; ) {
        String k = tags[i++];

        Object2ObjectHashMap<String, RoaringBitmap> bitmaps = tagIndexes.get(k);

        Object2ObjectHashMap<String, String> keyToTag = keyToTags.get(key);

        if (keyToTag == null) {
          continue;
        }

        String v = keyToTag.get(k);

        if (bitmaps != null) {
          RoaringBitmap bitmap = bitmaps.get(v);

          if (bitmap != null) {
            bitmap.remove(keyIndex);

            if (bitmap.isEmpty()) {
              bitmaps.remove(v);
            }
          }

          if (bitmaps.isEmpty()) {
            tagIndexes.remove(k);
          }
        }

        keyToTag.remove(k, v);

        if (keyToTag.isEmpty()) {
          keyToTags.remove(key);
        }
      }
    }

    Event<K> kEvent = Event.of(Event.EventType.REMOVE_TAG, key, Collections.EMPTY_MAP);
    events.onNext(kEvent);
  }
  // -- IndexableMap Methods --

  @Override
  public Iterable<Entry<K, V>> query(String... tags) {
    int length = tags.length;
    if (!BitUtil.isEven(length)) {
      throw new IllegalArgumentException("tags must be even");
    }

    RoaringBitmap result = new RoaringBitmap();
    synchronized (this) {
      for (int i = 0; i < length; ) {
        String k = tags[i++];
        String v = tags[i++];

        Object2ObjectHashMap<String, RoaringBitmap> indexes = tagIndexes.get(k);

        if (indexes == null) {
          return Collections.EMPTY_LIST;
        }

        RoaringBitmap bitmap = indexes.get(v);

        if (bitmap == null) {
          return Collections.EMPTY_LIST;
        }

        if (result.isEmpty()) {
          result.or(bitmap);
        } else {
          result.and(bitmap);
        }
      }
    }

    Iterator<Integer> iterator = result.iterator();
    return new QueryResultIterable(iterator);
  }

  private synchronized Entry<K, V> getEntry(int keyIndex) {
    K k = indexToKey.get(keyIndex);
    V v = delegate.get(k);

    return new Entry<K, V>() {
      @Override
      public K getKey() {
        return k;
      }

      @Override
      public V getValue() {
        return v;
      }

      @Override
      public V setValue(V value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Publisher<Event> events() {
    return events;
  }

  @Override
  public Publisher<Event> events(String... tags) {
    Objects.requireNonNull(tags);

    int length = tags.length;
    if (!BitUtil.isEven(length)) {
      throw new IllegalArgumentException("tags must be a power of two");
    }

    return events.filter(
        event -> {
          Map<String, String> map = event.getTags();
          if (map == null) {
            return false;
          }

          boolean found = false;
          for (int i = 0; i < length; ) {
            String k = tags[i++];
            String v = tags[i++];

            String value = map.get(k);
            if (value == null) {
              continue;
            }

            found = v.equals(value);
            if (found) {
              break;
            }
          }

          return found;
        });
  }

  @Override
  public Publisher<Event> eventsByTagKeys(String... tags) {
    Objects.requireNonNull(tags);

    int length = tags.length;
    return events.filter(
        event -> {
          Map<String, String> map = event.getTags();
          if (map == null) {
            return false;
          }

          boolean found = false;
          for (int i = 0; i < length; i++) {
            String k = tags[i];

            found = map.containsKey(k);
            if (found) {
              break;
            }
          }

          return found;
        });
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return delegate.get(key);
  }

  @Override
  public V put(K key, V value) {

    for (; ; ) {
      int nextKeyIndex = getNextKeyIndex();
      synchronized (this) {
        if (indexToKey.containsKey(nextKeyIndex)) {
          continue;
        }

        indexToKey.put(nextKeyIndex, key);
        keyToIndex.put(key, nextKeyIndex);
        break;
      }
    }

    Object o = delegate.put(key, value);

    Event.EventType type = o == null ? Event.EventType.ADD : Event.EventType.UPDATE;
    Event<K> kEvent = Event.of(type, key, Collections.EMPTY_MAP);
    events.onNext(kEvent);

    return (V) o;
  }

  @Override
  public V remove(Object key) {

    V remove = delegate.remove(key);

    if (remove != null) {
      Event event;
      synchronized (this) {
        int i = keyToIndex.removeKey((K) key);
        indexToKey.remove(i);

        Object2ObjectHashMap<String, String> keyToTag = keyToTags.get(key);

        if (keyToTag != null) {
          keyToTag.forEach((k, v) -> removeTags((K) key, k));
        }
        event = Event.of(Event.EventType.REMOVE, key, keyToTag);
      }

      events.onNext(event);
    }

    return remove;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.forEach(this::put);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    return delegate.keySet();
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }

  int getNextKeyIndex() {
    return (int) (KEY_INDEX.incrementAndGet(this) & mask);
  }

  private class QueryResultIterable implements Iterable<Entry<K, V>>, Iterator<Entry<K, V>> {
    private final Iterator<Integer> queryResults;

    public QueryResultIterable(Iterator<Integer> queryResults) {
      this.queryResults = queryResults;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return queryResults.hasNext();
    }

    @Override
    public Entry<K, V> next() {
      if (queryResults.hasNext()) {
        int next = queryResults.next();
        return getEntry(next);
      } else {
        throw new NoSuchElementException();
      }
    }
  }
}
