import events.*;
import models.*;
import models.Record;
import models.Timer;

import java.security.Key;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class Cache<KEY, VALUE> {
    private final Map<KEY, CompletionStage<Record<KEY, VALUE>>> cache;
    private final DataSource<KEY, VALUE> dataSource;
    private final PersistAlgorithm persistAlgorithm;
    private final EvictionAlgorithm evictionAlgorithm;
    private final int maximumSize;
    private final Duration expiryTime;
    private final ConcurrentSkipListMap<Long, List<KEY>> expiryQueue;
    private final ConcurrentSkipListMap<AccessDetails, List<KEY>> priorityQueue;
    private final ExecutorService threadPool[];
    private final int poolSize;
    private final List<Event<KEY, VALUE>> eventQueue;
    private final Timer timer;

    public Cache(final int maximumSize,
                 DataSource<KEY, VALUE> dataSource,
                 PersistAlgorithm persistAlgorithm,
                 EvictionAlgorithm evictionAlgorithm,
                 final Set<KEY> keysToEagerLoad,
                 Duration expiryTime, int poolSize,
                 final Timer timer) {
        this.dataSource = dataSource;
        this.persistAlgorithm = persistAlgorithm;
        this.evictionAlgorithm = evictionAlgorithm;
        this.expiryTime = expiryTime;
        this.poolSize = poolSize;
        this.eventQueue = new CopyOnWriteArrayList<>();
        this.expiryQueue = new ConcurrentSkipListMap<>();
        this.priorityQueue = new ConcurrentSkipListMap<>((first, second) -> {
            final var accessTimeDifference = (int) (first.getAccessTimestamp() - second.getAccessTimestamp());
            if(evictionAlgorithm.equals(EvictionAlgorithm.LRU)){
                return accessTimeDifference;
            }else{
                final var accessCountDifference = first.getAccessCount() - second.getAccessCount();
                return accessCountDifference != 0 ? (int) accessCountDifference : accessTimeDifference;
            }
        });
        cache = new ConcurrentHashMap<>();
        this.threadPool = new ExecutorService[5];
        for(int i=0; i<this.poolSize; i++){
            this.threadPool[i] = Executors.newSingleThreadExecutor();
        }
        final var eagerLoading = keysToEagerLoad.stream().map(key -> getThreadFor(key, addToCache(key, loadFromDB(dataSource, key))))
                .toArray(CompletableFuture[]::new);

        this.timer = timer;
        this.maximumSize = maximumSize;
    }
    private <U> CompletionStage<U> getThreadFor(KEY key, CompletionStage<U> task){
        return CompletableFuture.supplyAsync(()->task, threadPool[Math.abs(key.hashCode() % threadPool.length)]).thenCompose(Function.identity());
    }

    public CompletionStage<VALUE> get(KEY key){
        return getThreadFor(key, getFromCache(key));
    }

    private CompletionStage<VALUE> getFromCache(KEY key) {
        CompletionStage<Record<KEY, VALUE>> result;
        if(!cache.containsKey(key)){
            result = addToCache(key, loadFromDB(dataSource, key));
        }else{
            result = cache.get(key).thenCompose(record->{
                if (hasExpired(record)){
                    expiryQueue.get(record.getLoadTime()).remove(key);
                    priorityQueue.get(record.getAccessDetails()).remove(key);
                    eventQueue.add(new Eviction<>(record, Eviction.Type.EXPIRY, timer.getCurrentTime()));
                    return addToCache(key, loadFromDB(dataSource, key));
                }else{
                    return CompletableFuture.completedFuture(record);
                }
            });
        }
        return result.thenApply(record -> {
            priorityQueue.get(record.getAccessDetails()).remove(key);
            final var updatedAccessDetails = record.getAccessDetails().update(timer.getCurrentTime());
            priorityQueue.putIfAbsent(updatedAccessDetails, new CopyOnWriteArrayList<>());
            priorityQueue.get(updatedAccessDetails).add(key);
            record.setAccessDetails(updatedAccessDetails);
            return record.getValue();
        });
    }

    public CompletionStage<Void> set(KEY key, VALUE value){
        return getThreadFor(key, setInCache(key, value));
    }

    private CompletionStage<Void> setInCache(KEY key, VALUE value) {
        CompletionStage<Void> result = CompletableFuture.completedFuture(null);
        if(cache.containsKey(key)){
            result = cache.remove(key).thenAccept(oldRecord ->{
                priorityQueue.get(oldRecord.getAccessDetails()).remove(key);
                expiryQueue.get(oldRecord.getLoadTime()).remove(key);
                if(hasExpired(oldRecord)){
                    eventQueue.add(new Eviction<>(oldRecord, Eviction.Type.EXPIRY, timer.getCurrentTime()));
                }else{
                    eventQueue.add(new Update<>(new Record<>(key, value, timer.getCurrentTime()), oldRecord, timer.getCurrentTime()));
                }
            });
        }
        return result.thenCompose(__->addToCache(key, CompletableFuture.completedFuture(value))).thenCompose(record->{
            final CompletionStage<Void> writeOperation = persistRecord(record);
            return persistAlgorithm == PersistAlgorithm.WRITE_THROUGH ? writeOperation : CompletableFuture.completedFuture(null);
        });
    }

    private CompletionStage<Record<KEY, VALUE>> addToCache(KEY key, CompletionStage<VALUE> valueFuture){
        manageEntries();
        final var result = valueFuture.thenApply(value -> {
            Record<KEY, VALUE> valueRecord = new Record<>(key, value, timer.getCurrentTime());
            expiryQueue.putIfAbsent(valueRecord.getLoadTime(), new CopyOnWriteArrayList<>());
            expiryQueue.get(valueRecord.getLoadTime()).add(key);
            priorityQueue.putIfAbsent(valueRecord.getAccessDetails(), new CopyOnWriteArrayList<>());
            priorityQueue.get(valueRecord.getAccessDetails()).add(key);
            return valueRecord;
        });
        cache.put(key, result);
        return cache.get(key);
    }
    private synchronized void manageEntries(){
        if(cache.size() >= maximumSize){
            while(!expiryQueue.isEmpty() && hasExpired(expiryQueue.firstKey())){
                final List<KEY> expiredKeys = expiryQueue.pollFirstEntry().getValue();
                for(final KEY key: expiredKeys){
                    final Record<KEY, VALUE> expiredRecord = cache.remove(key).toCompletableFuture().join();
                    priorityQueue.remove(expiredRecord.getAccessDetails());
                    eventQueue.add(new Eviction<>(expiredRecord, Eviction.Type.EXPIRY, timer.getCurrentTime()));
                }
            }
            if(cache.size() >= maximumSize){
                List<KEY> keys = priorityQueue.pollFirstEntry().getValue();
                while (keys.isEmpty()) {
                    keys = priorityQueue.pollFirstEntry().getValue();
                }
                for (final KEY key: keys){
                    final Record<KEY, VALUE> lowestPriorityRecord = cache.remove(key).toCompletableFuture().join();
                    expiryQueue.get(lowestPriorityRecord.getLoadTime()).remove(lowestPriorityRecord.getKey());
                    eventQueue.add(new Eviction<>(lowestPriorityRecord, Eviction.Type.REPLACEMENT, timer.getCurrentTime()));
                }
            }
        }
    }
    private CompletionStage<VALUE> loadFromDB(final DataSource<KEY, VALUE> dataSource, KEY key){
        return dataSource.load(key).whenComplete(((value, throwable) -> {
            if (throwable == null){
                eventQueue.add(new Load<>(new Record<>(key, value, timer.getCurrentTime()), timer.getCurrentTime()));
            }
        }));
    }
    private boolean hasExpired(final Record<KEY, VALUE> record) {
        return hasExpired(record.getLoadTime());
    }

    private boolean hasExpired(final Long time) {
        return Duration.ofNanos(timer.getCurrentTime() - time).compareTo(expiryTime) > 0;
    }
    private CompletionStage<Void> persistRecord(final Record<KEY, VALUE> record){
        return dataSource.persist(record.getKey(), record.getValue(), record.getLoadTime())
                .thenAccept(__->eventQueue.add(new Write<>(record, timer.getCurrentTime())));
    }

}

