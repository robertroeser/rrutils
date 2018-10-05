package rrutils.collection;

import org.agrona.collections.Object2ObjectHashMap;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class IndexableMapWrapperTest {
  @Test
  public void testMutationsWithoutTags() {
    Map<String, String> map = new HashMap<>();
    Map<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    String put = wrappedMap.put("Hello", "World");

    Assert.assertNull(put);

    String hello = map.get("Hello");

    Assert.assertEquals("World", hello);

    String hello1 = wrappedMap.get("Hello");
    Assert.assertEquals(hello, hello1);

    wrappedMap.remove("Hello");

    Assert.assertTrue(wrappedMap.isEmpty());
    Assert.assertTrue(map.isEmpty());
  }

  @Test
  public void testMutationWithTags() {
    Map<String, String> map = new HashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    String put = wrappedMap.put("Hello", "World", "aTag", "value");

    Assert.assertNull(put);

    String hello = map.get("Hello");

    Assert.assertEquals("World", hello);

    wrappedMap.removeTags("Hello", "aTag", "value");

    Assert.assertTrue(map.containsKey("Hello"));

    wrappedMap.addTags("Hello", "newTag", "newValue");

    Assert.assertTrue(map.containsKey("Hello"));

    wrappedMap.remove("Hello");

    Assert.assertFalse(map.containsKey("Hello"));
  }

  @Test(expected = IllegalStateException.class)
  public void testThrowsMissingKeyExceptionWhenAddingTagWithoutKeyPresent() {
    Map<String, String> map = new HashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    wrappedMap.addTags("notThere", "aTag", "value");
  }

  @Test
  public void testQuerySingleTag() {
    Map<String, String> map = new HashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    wrappedMap.put("Hello", "World", "aTag", "value");

    Iterable<Map.Entry<String, String>> query = wrappedMap.query("aTag", "value");

    Iterator<Map.Entry<String, String>> iterator = query.iterator();

    Assert.assertTrue(iterator.hasNext());
    Map.Entry<String, String> next = iterator.next();
    Assert.assertEquals("Hello", next.getKey());
  }

  @Test
  public void testQueryMultipleTags() {
    Map<String, String> map = new HashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    wrappedMap.put("Hello", "World", "aTag", "value", "aTag2", "value", "aTag3", "value");

    Iterable<Map.Entry<String, String>> query = wrappedMap.query("aTag", "value", "aTag3", "value");

    Iterator<Map.Entry<String, String>> iterator = query.iterator();

    Assert.assertTrue(iterator.hasNext());
    Map.Entry<String, String> next = iterator.next();
    Assert.assertEquals("Hello", next.getKey());
  }

  @Test
  public void testQuerySingleTagNotFound() {
    Map<String, String> map = new HashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    wrappedMap.put("Hello", "World", "aTag", "value");

    Iterable<Map.Entry<String, String>> query = wrappedMap.query("aTag", "won't find me");

    Iterator<Map.Entry<String, String>> iterator = query.iterator();

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testQuerySingleTagAddAndRemove() {
    Map<String, String> map = new HashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    wrappedMap.put("Hello", "World");

    Iterable<Map.Entry<String, String>> query = wrappedMap.query("aTag", "value");
    Iterator<Map.Entry<String, String>> iterator = query.iterator();

    Assert.assertFalse(iterator.hasNext());

    wrappedMap.addTags("Hello", "aTag", "value");

    query = wrappedMap.query("aTag", "value");
    iterator = query.iterator();

    Assert.assertTrue(iterator.hasNext());
    
    wrappedMap.removeTags("Hello", "aTag");
    
    query = wrappedMap.query("aTag", "value");
    iterator = query.iterator();
  
    Assert.assertFalse(iterator.hasNext());
    
  }

  public void testQueryFind_N(int count) {
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    long start = System.nanoTime();
    for (int i = 0; i < count; i++) {
      wrappedMap.put("Hello" + i, "World", "aTag", "value");
    }
    long time = System.nanoTime() - start;
    System.out.println("insert time => " + TimeUnit.NANOSECONDS.toMicros(time) + "us");

    start = System.nanoTime();
    long found =
        StreamSupport.stream(wrappedMap.query("aTag", "value").spliterator(), false).count();
    time = System.nanoTime() - start;
    System.out.println("query time time => " + TimeUnit.NANOSECONDS.toMicros(time) + "us");

    Assert.assertEquals(count, found);
  }

  @Test
  public void testQueryFind10() {
    testQueryFind_N(10);
  }

  @Test
  public void testQueryFind100() {
    testQueryFind_N(100);
  }

  @Test
  public void testQueryFind10_000() {
    testQueryFind_N(10_000);
  }

  @Test
  public void testQueryFind100_000() {
    testQueryFind_N(100_000);
  }

  @Test
  public void testQueryFind1_000_000() {
    testQueryFind_N(1_000_000);
  }

  @Test
  public void testMultiTagQueryFind10() {
    testMultiTagQueryFind_N(10);
  }

  @Test
  public void testMultiTagQueryFind100() {
    testMultiTagQueryFind_N(100);
  }

  @Test
  public void testMultiTagQueryFind10_000() {
    testMultiTagQueryFind_N(10_000);
  }

  @Test
  public void testMultiTagQueryFind100_000() {
    testMultiTagQueryFind_N(100_000);
  }

  @Test
  public void testMultiTagQueryFind1_000_000() {
    testMultiTagQueryFind_N(1_000_000);
  }

  public void testMultiTagQueryFind_N(int count) {
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);

    long start = System.nanoTime();
    for (int i = 0; i < count; i++) {
      wrappedMap.put("Hello" + i, "World", "aTag", "value", "aTag2", "value", "aTag3", "value");
    }
    long time = System.nanoTime() - start;
    System.out.println("insert time => " + TimeUnit.NANOSECONDS.toMicros(time) + "us");

    start = System.nanoTime();
    long found =
        StreamSupport.stream(
                wrappedMap.query("aTag", "value", "aTag3", "value").spliterator(), false)
            .count();
    time = System.nanoTime() - start;
    System.out.println("query time time => " + TimeUnit.NANOSECONDS.toMicros(time) + "us");

    Assert.assertEquals(count, found);
  }

  @Test
  public void testShouldEmitEventOnPutNoTags() {
    ArrayDeque<IndexableMap.Event> events = new ArrayDeque<>();
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);
    Flux.from(wrappedMap.events()).doOnNext(events::add).subscribe();

    wrappedMap.put("Hello", "World");

    Assert.assertFalse(events.isEmpty());
    IndexableMap.Event poll = events.poll();
    Assert.assertEquals("Hello", poll.getKey());
    Assert.assertEquals(IndexableMap.Event.EventType.ADD, poll.getType());
    Assert.assertTrue(events.isEmpty());
  }
  
  @Test
  public void testShouldEmitEventOnPutWithTags() {
    ArrayDeque<IndexableMap.Event> events = new ArrayDeque<>();
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);
    Flux.from(wrappedMap.events()).doOnNext(events::add).subscribe();
    
    wrappedMap.put("Hello", "World", "aTag", "value");
    
    Assert.assertFalse(events.isEmpty());
    IndexableMap.Event poll = events.poll();
    Assert.assertEquals("Hello", poll.getKey());
    Assert.assertEquals(IndexableMap.Event.EventType.ADD, poll.getType());
    Assert.assertFalse(events.isEmpty());
    poll = events.poll();
    Assert.assertEquals(IndexableMap.Event.EventType.ADD_TAG, poll.getType());
  }
  
  @Test
  public void testShouldEmitWhenTagsAreAdded() {
    ArrayDeque<IndexableMap.Event> events = new ArrayDeque<>();
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);
    Flux.from(wrappedMap.events()).doOnNext(events::add).subscribe();
    
    wrappedMap.put("Hello", "World");
    
    Assert.assertFalse(events.isEmpty());
    IndexableMap.Event poll = events.poll();
    Assert.assertEquals("Hello", poll.getKey());
    Assert.assertEquals(IndexableMap.Event.EventType.ADD, poll.getType());
    Assert.assertTrue(events.isEmpty());
    
    for (int i = 0; i < 10; i++) {
      wrappedMap.addTags("Hello","aTag" + i, "value");
    }
    
    Assert.assertEquals(10, events.size());
  }
  
  @Test
  public void testShouldEmitOnRemoveNoTags() {
    ArrayDeque<IndexableMap.Event> events = new ArrayDeque<>();
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);
    Flux.from(wrappedMap.events()).doOnNext(events::add).subscribe();
  
    wrappedMap.put("Hello", "World");
  
    Assert.assertFalse(events.isEmpty());
    IndexableMap.Event poll = events.poll();
    Assert.assertEquals("Hello", poll.getKey());
    Assert.assertEquals(IndexableMap.Event.EventType.ADD, poll.getType());
    Assert.assertTrue(events.isEmpty());
    
    wrappedMap.remove("Hello");
  
    Assert.assertFalse(events.isEmpty());
    poll = events.poll();
    Assert.assertEquals("Hello", poll.getKey());
    Assert.assertEquals(IndexableMap.Event.EventType.REMOVE, poll.getType());
    Assert.assertTrue(events.isEmpty());
  }
  
  @Test
  public void testShouldEmitEventOnRemoveTags() {
    ArrayDeque<IndexableMap.Event> events = new ArrayDeque<>();
    Map<String, String> map = new Object2ObjectHashMap<>();
    IndexableMap<String, String> wrappedMap = IndexableMapWrapper.wrap(map);
    Flux.from(wrappedMap.events()).doOnNext(events::add).subscribe();
    
    wrappedMap.put("Hello", "World", "aTag", "value", "aTag1", "value");
    
    Assert.assertFalse(events.isEmpty());
    Assert.assertEquals(2, events.size());
    IndexableMap.Event event = events.pollLast();
    Assert.assertEquals("Hello", event.getKey());
    Assert.assertEquals(IndexableMap.Event.EventType.ADD_TAG, event.getType());
    
    events.clear();
    
    wrappedMap.removeTags("Hello", "aTag");
    wrappedMap.removeTags("Hello", "aTag1");
    Assert.assertFalse(events.isEmpty());
    event = events.poll();
    Assert.assertEquals(IndexableMap.Event.EventType.REMOVE_TAG, event.getType());
    event = events.poll();
    Assert.assertEquals(IndexableMap.Event.EventType.REMOVE_TAG, event.getType());
    Assert.assertTrue(events.isEmpty());
  }
  
}
