import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

public interface DataSource<KEY, VALUE> {
    public CompletionStage<VALUE> load(KEY key);
    public CompletionStage<Void> persist(KEY key, VALUE value, long timestamp);
}
