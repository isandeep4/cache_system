package models;

import java.util.concurrent.atomic.LongAdder;

public class AccessDetails {
    private long accessTimestamp;
    private final LongAdder accessCount;
    public AccessDetails(long accessTimestamp) {
        accessCount = new LongAdder();
        this.accessTimestamp = accessTimestamp;
    }

    public long getAccessTimestamp() {
        return accessTimestamp;
    }

    public int getAccessCount() {
        return (int) accessCount.longValue();
    }

    public AccessDetails update(long currentTime) {
        AccessDetails accessDetails = new AccessDetails(currentTime);
        accessDetails.accessCount.add(this.accessCount.longValue()+1);
        return accessDetails;
    }
}
