import org.junit.Before;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

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

}
