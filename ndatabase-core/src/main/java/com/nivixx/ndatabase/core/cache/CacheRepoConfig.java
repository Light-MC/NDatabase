package com.nivixx.ndatabase.core.cache;

public class CacheRepoConfig {
    private long expireAfterAccessMinutes;
    private long maximumSize;
    private int getTimeoutSeconds;
    private boolean enableSoftValues;
    private boolean enableRefreshAfterWrite;
    private long refreshAfterWriteMinutes;
    private boolean enableStats;

    public long getExpireAfterAccessMinutes() {
        return expireAfterAccessMinutes;
    }

    public void setExpireAfterAccessMinutes(long expireAfterAccessMinutes) {
        this.expireAfterAccessMinutes = expireAfterAccessMinutes;
    }

    public long getMaximumSize() {
        return maximumSize;
    }

    public void setMaximumSize(long maximumSize) {
        this.maximumSize = maximumSize;
    }

    public int getGetTimeoutSeconds() {
        return getTimeoutSeconds;
    }

    public void setGetTimeoutSeconds(int getTimeoutSeconds) {
        this.getTimeoutSeconds = getTimeoutSeconds;
    }

    public boolean isEnableSoftValues() {
        return enableSoftValues;
    }

    public void setEnableSoftValues(boolean enableSoftValues) {
        this.enableSoftValues = enableSoftValues;
    }

    public boolean isEnableRefreshAfterWrite() {
        return enableRefreshAfterWrite;
    }

    public void setEnableRefreshAfterWrite(boolean enableRefreshAfterWrite) {
        this.enableRefreshAfterWrite = enableRefreshAfterWrite;
    }

    public long getRefreshAfterWriteMinutes() {
        return refreshAfterWriteMinutes;
    }

    public void setRefreshAfterWriteMinutes(long refreshAfterWriteMinutes) {
        this.refreshAfterWriteMinutes = refreshAfterWriteMinutes;
    }

    public boolean isEnableStats() {
        return enableStats;
    }

    public void setEnableStats(boolean enableStats) {
        this.enableStats = enableStats;
    }
}
