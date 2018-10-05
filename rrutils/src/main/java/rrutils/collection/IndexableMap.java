package rrutils.collection;

import org.reactivestreams.Publisher;

import java.util.Map;
import java.util.Objects;

/**
 * A {@link Map} wrapper that lets you add tags to entries that a query via Bitmap indexes.
 * Additionally it will emit events when tags and entries are mutated. The wrapped class is only
 * thread-safe if the wrapped map is thread-safe. Once a map is wrapped it is not longer safe to
 * mutate the map. If you do so it could lead to corrupt indexes in the wrapper.
 *
 *
 * @see Map
 *
 * @author Robert Roeser
 */
public abstract class IndexableMap<K, V> implements Map<K, V> {
  /**
   * Wraps an map
   *
   * @param target the map to wrapped
   * @param <K>
   * @param <V>
   * @return a IndexableMap
   */
  public static <K, V> IndexableMap<K, V> wrap(Map<K, V> target) {
    return new IndexableMapWrapper<>(target);
  }

  public abstract V put(K key, V value, String... tags);

  public abstract void addTags(K key, String... tags);

  public abstract void removeTags(K key, String... tags);

  public abstract Iterable<Entry<K, V>> query(String... tags);

  public abstract Publisher<Event> events();

  public abstract Publisher<Event> events(String... tags);

  public abstract Publisher<Event> eventsByTagKeys(String... tags);

  public static class Event<K> {
    private final EventType type;
    private final K key;
    private final Map<String, String> tags;

    private Event(EventType type, K key, Map<String, String> tags) {
      this.type = type;
      this.key = key;
      this.tags = tags;
    }

    public static <K> Event of(EventType eventType, K key, Map<String, String> tags) {
      return new Event(eventType, key, tags);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Event event = (Event) o;
      return type == event.type && Objects.equals(key, event.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, key);
    }

    public EventType getType() {
      return type;
    }

    public K getKey() {
      return key;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    enum EventType {
      ADD,
      REMOVE,
      UPDATE,
      ADD_TAG,
      REMOVE_TAG
    }
  }
}
