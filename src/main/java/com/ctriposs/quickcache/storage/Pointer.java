package com.ctriposs.quickcache.storage;


import com.ctriposs.quickcache.IBlock;

public class Pointer {

	private final IBlock block;
	private final int metaOffset;
	private final int keySize;
	private final int valueSize;
	private final long lastAccessTime; 
	private final long ttl;			  // -1 means for never expire;0 means for delete
	private final long createNanoTime;
	
	public Pointer(IBlock block, int metaOffset, int keySize, int valueSize, long ttl) {
		this.block = block;
		this.metaOffset = metaOffset;
		this.ttl = ttl;
		this.lastAccessTime = System.currentTimeMillis();
		this.keySize = keySize;
		this.valueSize = valueSize;
		this.createNanoTime = System.nanoTime();
	}

	public Pointer(IBlock block, int metaOffset, int keySize, int valueSize, long ttl, long lastAccessTime) {
		
		this.block = block;
		this.metaOffset = metaOffset;
		this.ttl = ttl;
		this.keySize = keySize;
		this.valueSize = valueSize;
		this.lastAccessTime = lastAccessTime;
		this.createNanoTime = System.nanoTime();
	}
	
	public IBlock getBlock() {
		return block;
	}

	public int getMetaOffset() {
		return metaOffset;
	}

	public long getLastAccessTime() {
		return lastAccessTime;
	}

	public long getTtl() {
		return ttl;
	}
	
    /**
     * Is the cached item expired
     *
     * @return expired or not
     */
    public boolean isExpired() {
	    if (ttl < 0) return false; 				// never expire
	    if (lastAccessTime < 0) return false; 		// not initialized
	    return System.currentTimeMillis() - lastAccessTime > ttl;
    }
    
	public int getKeySize() {
		return keySize;
	}

	public int getValueSize() {
		return valueSize;
	}

	public int getItemSize() {
		return (this.keySize + this.valueSize);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Pointer) {
			Pointer other = (Pointer) obj;
			if (this.block == other.block && 
				this.lastAccessTime == other.lastAccessTime) {
				return true;
			}
		}
		return false;
	}
	public long getCreateNanoTime() {
		return createNanoTime;
	}
}
