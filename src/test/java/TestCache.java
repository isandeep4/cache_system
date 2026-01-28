import events.Eviction;
import events.Load;
import events.Update;
import events.Write;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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
