package com.threathunter.greyhound.server.engine;

import com.threathunter.variable.DimensionType;

import java.util.Set;

/**
 * 
 */
public class EngineConfiguration {
    private int slidingWidthInSec = 5 * 60;
    private int slotWidthInMin = 1;
    private boolean loseTolerant = true;

    private Set<DimensionType> batchModeDimensions;
    private Set<String> batchModeEventNames;
    private Set<DimensionType> enableDimensions;
    private boolean sync = false;

    private boolean redisBabel = true;
    private int shardCount = 1;
    private int threadCount = 1;
    private int capacity = 10000;
    private int noticeSyncExpireSeconds = 9;

    public boolean isRedisBabel() {
        return redisBabel;
    }

    public void setRedisBabel(boolean redisBabel) {
        this.redisBabel = redisBabel;
    }

    public Set<DimensionType> getBatchModeDimensions() {
        return batchModeDimensions;
    }

    public void setBatchModeDimensions(Set<DimensionType> batchModeDimensions) {
        this.batchModeDimensions = batchModeDimensions;
    }

    public Set<String> getBatchModeEventNames() {
        return batchModeEventNames;
    }

    public void setBatchModeEventNames(Set<String> batchModeEventNames) {
        this.batchModeEventNames = batchModeEventNames;
    }

    public Set<DimensionType> getEnableDimensions() {
        return enableDimensions;
    }

    public void setEnableDimensions(Set<DimensionType> enableDimensions) {
        this.enableDimensions = enableDimensions;
    }

    public int getSlotWidthInMin() {
        return slotWidthInMin;
    }

    public void setSlotWidthInMin(int slotWidthInMin) {
        this.slotWidthInMin = slotWidthInMin;
    }

    public int getSlidingWidthInSec() {
        return slidingWidthInSec;
    }

    public void setSlidingWidthInSec(int slidingWidthInSec) {
        this.slidingWidthInSec = slidingWidthInSec;
    }

    public boolean isLoseTolerant() {
        return loseTolerant;
    }

    public void setLoseTolerant(boolean loseTolerant) {
        this.loseTolerant = loseTolerant;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getNoticeSyncExpireSeconds() {
        return noticeSyncExpireSeconds;
    }

    public void setNoticeSyncExpireSeconds(int noticeSyncExpireSeconds) {
        this.noticeSyncExpireSeconds = noticeSyncExpireSeconds;
    }
}
