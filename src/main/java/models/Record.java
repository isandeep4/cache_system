package models;

public class Record<KEY, VALUE> {
    private final KEY key;
    private final VALUE value;
    private AccessDetails accessDetails;

    private long loadTime;

    public Record(KEY key, VALUE value, long loadTime) {
        this.key = key;
        this.value = value;
        this.accessDetails = new AccessDetails(loadTime);
        this.loadTime = loadTime;
    }

    public AccessDetails getAccessDetails() {
        return accessDetails;
    }

    public VALUE getValue() {
        return value;
    }

    public long getLoadTime() {
        return loadTime;
    }
    public void setAccessDetails(AccessDetails accessDetails) {
        this.accessDetails = accessDetails;
    }
    public KEY getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + key +
                ", value=" + value +
                ", accessDetails=" + accessDetails +
                ", loadTime=" + loadTime +
                '}';
    }
}
