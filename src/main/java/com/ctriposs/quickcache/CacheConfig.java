package com.ctriposs.quickcache;

import com.ctriposs.quickcache.storage.StorageManager;



public class CacheConfig {
	
	private int concurrencyLevel = QuickCache.DEFAULT_CONCURRENCY_LEVEL;
	private int capacityPerBlock = StorageManager.DEFAULT_CAPACITY_PER_BLOCK;
	private int initialNumberOfBlocks = StorageManager.DEFAULT_INITIAL_NUMBER_OF_BLOCKS;
    private long maxOffHeapMemorySize = StorageManager.DEFAULT_MAX_OFFHEAP_MEMORY_SIZE;	
    private long expireInterval = QuickCache.DEFAULT_EXPIRE_INTERVAL;
    private long migrateInterval = QuickCache.DEFAULT_MIGRATE_INTERVAL;
    private double dirtyRatioThreshold = QuickCache.DEFAULT_DIRTY_RATIO_THRESHOLD;
    private StorageMode storageMode = StorageMode.PureFile;
    private StartMode startMode = StartMode.ClearOldFile;
    
	public int getCapacityPerBlock() {
		return capacityPerBlock;
	}
	
	public int getConcurrencyLevel() {
		return concurrencyLevel;
	}
	
	public CacheConfig setCapacityPerBlock(int capacityPerBlock) {
		if(capacityPerBlock < 16 * 1024 * 1024){
			throw new IllegalArgumentException("capacityPerBlock must be bigger than 16MB!");
		}
		
		this.capacityPerBlock = capacityPerBlock;
		return this;
	}

	public int getInitialNumberOfBlocks() {
		return initialNumberOfBlocks;
	}

	public CacheConfig setInitialNumberOfBlocks(int initialNumberOfBlocks) {
		
		if(initialNumberOfBlocks <= 0){
			throw new IllegalArgumentException("initialNumberOfBlocks must be > 0!");
		}
		
		this.initialNumberOfBlocks = initialNumberOfBlocks;
		return this;
	}

    public long getExpireInterval() {
        return expireInterval;
    }

    public long getMigrateInterval() {
		return migrateInterval;
	}

	public CacheConfig setMigrateInterval(long migrateInterval) {
		this.migrateInterval = migrateInterval;
		return this;
	}

	public CacheConfig setExpireInterval(long expireInterval) {
        this.expireInterval = expireInterval;
        return this;
    }

    public double getDirtyRatioThreshold() {
        return dirtyRatioThreshold;
    }

    public CacheConfig setDirtyRatioLimit(double dirtyRatioThreshold) {
        this.dirtyRatioThreshold = dirtyRatioThreshold;
        return this;
    }

	public StorageMode getStorageMode() {
		return storageMode;
	}

	public CacheConfig setStorageMode(StorageMode storageMode) {
		this.storageMode = storageMode;
		return this;
	}

	public StartMode getStartMode() {
		return startMode;
	}

	public void setStartMode(StartMode startMode) {
		this.startMode = startMode;
	}

	/**
	 * Limiting Offheap memory usage.
	 * 
	 * Only takes effect when the {@link StorageMode} is set to MemoryMappedPlusFile or OffHeapPlusFile mode,
	 * in these cases, this setting limits the max offheap memory size.
	 * 
	 * @param maxOffHeapMemorySize max offheap memory size allowed, unit : byte.
	 * @return CacheConfig
	 */
	public CacheConfig setMaxOffHeapMemorySize(long maxOffHeapMemorySize) {
		if (maxOffHeapMemorySize < this.capacityPerBlock) {
			throw new IllegalArgumentException("maxOffHeapMemorySize must be equal to or larger than capacityPerBlock" + this.getCapacityPerBlock());
		}
		this.maxOffHeapMemorySize = maxOffHeapMemorySize;
		return this;
	}

	public long getMaxOffHeapMemorySize() {
		return this.maxOffHeapMemorySize;
	}   
	
	public enum StorageMode {
		PureFile,
		MapFile,
		OffHeapFile,
	}
	
	public enum StartMode {
		ClearOldFile,
		RecoveryFromFile
	}
}
