import events.Eviction;
import events.Load;
import events.Update;
import events.Write;
import models.SettableTimer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TestCache {
    private static final String USER_ID = "user_id", PASSWORD = "password";
    private final Map<String, String> dataMap = new ConcurrentHashMap<>();
    private DataSource<String, String> dataSource;
    private final Queue<CompletableFuture<Void>> writeOperations = new LinkedList<>();
    private DataSource<String, String> writeBackDataSource;

    @Before
    public void setUp(){
        dataMap.clear();
        writeOperations.clear();
        dataMap.put(USER_ID, "12345");
        dataMap.put(PASSWORD, "@password");
        dataSource = new DataSource<String, String>() {
            @Override
            public CompletionStage<String> load(String key) {
                if(dataMap.containsKey(key)){
                    return CompletableFuture.completedFuture(dataMap.get(key));
                }else{
                    return CompletableFuture.failedStage(new NullPointerException());
                }
            }

            @Override
            public CompletionStage<Void> persist(String key, String value, long timestamp) {
                dataMap.put(key, value);
                return CompletableFuture.completedFuture(null);
            }
        };
        writeBackDataSource = new DataSource<String, String>() {
            @Override
            public CompletionStage<String> load(String key) {
                if(dataMap.containsKey(key)){
                    return CompletableFuture.completedFuture(dataMap.get(key));
                }else{
                    return CompletableFuture.failedStage(new NullPointerException());
                }
            }

            @Override
            public CompletionStage<Void> persist(String key, String value, long timestamp) {
                final CompletableFuture<Void> hold = new CompletableFuture<>();
                writeOperations.add(hold);
                return hold.thenAccept(___->dataMap.put(key,value));
            }
        };
    }
    private void acceptWrite(){
        CompletableFuture<Void> write = writeOperations.poll();
        if(write != null){
            write.complete(null);
        }
    }
    @Test(expected = IllegalArgumentException.class)
    public void testCacheConstructionWithoutDataSourceFailure() {
        new CacheBuilder<>().build();
    }
    @Test
    public void testCacheDefaultBehaviour() throws ExecutionException, InterruptedException{
        final var cache = new CacheBuilder<String, String>().dataSource(dataSource).build();
        Assert.assertNotNull(cache);
        assert isEqualTo(cache.get(USER_ID), "12345");
        assert isEqualTo(cache.set(USER_ID, "987654").thenCompose(__-> cache.get(USER_ID)), "987654");
        Assert.assertEquals(3, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        assert cache.getEventQueue().get(1) instanceof Update;
        assert cache.getEventQueue().get(2) instanceof Write;
    }

    @Test
    public void Eviction_LRU(){
        final var cache = new CacheBuilder<String, String>()
                .maximumSize(2)
                .evictionAlgorithm(EvictionAlgorithm.LRU)
                .persistAlgorithm(PersistAlgorithm.WRITE_BACK)
                .dataSource(writeBackDataSource)
                .build();
        cache.get(USER_ID).toCompletableFuture().join();
        for (int i=0; i<2; i++){
            cache.set("key"+i, "value"+i).toCompletableFuture().join();
        }
        Assert.assertEquals(2, cache.getEventQueue().size());
        assert cache.getEventQueue().get(0) instanceof Load;
        assert cache.getEventQueue().get(1) instanceof Eviction;
        final var evictionEvent = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.REPLACEMENT, evictionEvent.getType());
        Assert.assertEquals(USER_ID, evictionEvent.getElement().getKey());
    }
    @Test
    public void ExpiryOnGet(){
        final var timer = new SettableTimer();
        final var startTime = System.nanoTime();
        final var cache = new CacheBuilder<String, String>()
                .timer(timer)
                .dataSource(dataSource)
                .expiryTime(Duration.ofSeconds(10))
                .build();
        timer.setTime(startTime);
        cache.get(USER_ID).toCompletableFuture().join();
        Assert.assertEquals(1, cache.getEventQueue().size());
        assert  cache.getEventQueue().get(0) instanceof Load;
        Assert.assertEquals(USER_ID, cache.getEventQueue().get(0).getElement().getKey());
        timer.setTime(startTime + Duration.ofSeconds(10).toNanos() + 1);
        cache.get(USER_ID).toCompletableFuture().join();
        Assert.assertEquals(3, cache.getEventQueue().size());
        assert cache.getEventQueue().get(1) instanceof Eviction;
        assert cache.getEventQueue().get(2) instanceof Load;
        final var eviction = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.EXPIRY, eviction.getType());
        Assert.assertEquals(USER_ID, eviction.getElement().getKey());
    }
    @Test
    public void ExpiryOnSet(){
        final var timer = new SettableTimer();
        final var startTime = System.nanoTime();
        final var cache = new CacheBuilder<String, String>()
                .timer(timer)
                .dataSource(dataSource)
                .expiryTime(Duration.ofSeconds(10))
                .build();
        timer.setTime(startTime);
        cache.get(USER_ID).toCompletableFuture().join();
        Assert.assertEquals(1, cache.getEventQueue().size());
        assert  cache.getEventQueue().get(0) instanceof Load;
        Assert.assertEquals(USER_ID, cache.getEventQueue().get(0).getElement().getKey());
        timer.setTime(startTime + Duration.ofSeconds(10).toNanos() + 1);
        cache.set(USER_ID, "98765").toCompletableFuture().join();
        Assert.assertEquals(3, cache.getEventQueue().size());
        assert cache.getEventQueue().get(1) instanceof Eviction;
        assert cache.getEventQueue().get(2) instanceof Write;
        final var eviction = (Eviction<String, String>) cache.getEventQueue().get(1);
        Assert.assertEquals(Eviction.Type.EXPIRY, eviction.getType());
        Assert.assertEquals(USER_ID, eviction.getElement().getKey());
    }

    @Test
    public void ExpiryOnEviction(){
        final var timer = new SettableTimer();
        final var startTime = System.nanoTime();
        final var cache = new CacheBuilder<String, String>()
                .maximumSize(2)
                .timer(timer)
                .dataSource(dataSource)
                .expiryTime(Duration.ofSeconds(10))
                .build();
        timer.setTime(startTime);
        cache.get(USER_ID).toCompletableFuture().join();
        cache.get(PASSWORD).toCompletableFuture().join();
        Assert.assertEquals(2, cache.getEventQueue().size());
        assert  cache.getEventQueue().get(0) instanceof Load;
        assert  cache.getEventQueue().get(1) instanceof Load;
        timer.setTime(startTime + Duration.ofSeconds(10).toNanos() + 1);
        cache.set("name", "Sandeep").toCompletableFuture().join();
        Assert.assertEquals(5, cache.getEventQueue().size());
        assert cache.getEventQueue().get(2) instanceof Eviction;
        assert cache.getEventQueue().get(3) instanceof Eviction;
        assert cache.getEventQueue().get(4) instanceof Write;
    }
    @Test
    public void RaceConditions(){
        final var cache = new CacheBuilder<String, String>()
                .poolSize(8)
                .dataSource(dataSource)
                .build();
        final var cacheEntries = new HashMap<String, List<String>>();
        final var numberOfEntries = 100;
        final var numberOfValues = 1000;
        final String[] keyList = new String[numberOfEntries];
        final Map<String, Integer> inverseMapping = new HashMap<>();
        for(int entry=0; entry<numberOfEntries; entry++){
            final var key = UUID.randomUUID().toString();
            keyList[entry] = key;
            inverseMapping.put(key, entry);
            cacheEntries.put(key, new ArrayList<>());
            final var firstValue = UUID.randomUUID().toString();
            dataMap.put(key, firstValue);
            cacheEntries.get(key).add(firstValue);
            for(int value = 0; value < numberOfValues - 1; value++){
                cacheEntries.get(key).add(UUID.randomUUID().toString());
            }
        }
        final Random random = new Random();
        final List<CompletionStage<String>> futures = new ArrayList<>();
        final List<String> queries = new ArrayList<>();
        final int[] updates = new int[numberOfEntries];
        for (int i=0; i < 1000000; i++){
            final var index = random.nextInt(numberOfEntries);
            final var key = keyList[index];
            if(Math.random() <= 0.05){
                if(updates[index] -1 < numberOfEntries){
                    updates[index]++;
                }
                cache.set(key, cacheEntries.get(key).get(updates[index]+1));
            }else{
                queries.add(key);
                futures.add(cache.get(key));
            }
        }
        final CompletionStage<List<String>> results = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(__->futures.stream()
                        .map(CompletionStage::toCompletableFuture)
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList()));
        final int[] currentIndexes = new int[numberOfEntries];

    }


    private boolean isEqualTo(CompletionStage<String> futureValue, String testValue) {
        return futureValue.thenApply(value -> {
            if(value.equals(testValue)){
                return true;
            }else {
                throw new AssertionError();
            }
        }).toCompletableFuture().join();
    }


}
