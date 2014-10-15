package com.ctriposs.quickcache.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.ctriposs.quickcache.CacheConfig.StorageMode;
import com.ctriposs.quickcache.IBlock;
import com.ctriposs.quickcache.IStorage;
import com.ctriposs.quickcache.QuickCache;
import com.ctriposs.quickcache.utils.ByteUtil;



public class StorageBlock implements IBlock {
	
	/** The index. */
	private final int index;
	
	/** The capacity. */
	private final int capacity;
	
	/** The dirty storage. */
	private final AtomicInteger dirtyStorage = new AtomicInteger(0);
	
	/** The used storage. */
	private final AtomicInteger usedStorage = new AtomicInteger(0);
	
	/** The underlying storage. */
	private IStorage underlyingStorage;	
	
	/** The item offset within the active block.*/
	private final AtomicInteger currentItemOffset = new AtomicInteger(0);
	
	/**
	 * Instantiates a new storage block.
	 *
	 * @param dir the directory
	 * @param index the index
	 * @param capacity the capacity
	 * @throws IOException exception throws when failing to create the storage block
	 */
	public StorageBlock(String dir, int index, int capacity, StorageMode storageMode) throws IOException{
		this.index = index;
		this.capacity = capacity;
		switch (storageMode) {
		case PureFile:
			this.underlyingStorage = new PureFileStorage(dir, index, capacity);
			break;
		case MapFile:
			this.underlyingStorage = new MapFileStorage(dir, index, capacity);
			break;
		case OffHeapFile:
			this.underlyingStorage = new OffHeapStorage(capacity);
			break;
		}
	}
	
	/**
	 * Load existed storage block from file.
	 *
	 * @param file the directory
	 * @param index the index
	 * @param capacity the capacity
	 * @throws IOException exception throws when failing to create the storage block
	 */
	public StorageBlock(File file, int index, int capacity, StorageMode storageMode) throws IOException{
		this.index = index;
		this.capacity = capacity;
		switch (storageMode) {
            case PureFile:
                this.underlyingStorage = new PureFileStorage(file, capacity);
                break;
            case MapFile:
                this.underlyingStorage = new MapFileStorage(file, capacity);
                break;
            case OffHeapFile:
                this.underlyingStorage = new OffHeapStorage(capacity);
                break;
		}
	}
	
	/**
	 * Stores the payload by the help of allocation.
	 *
	 * @param allocation the allocation
	 * @param payloadLength the payload
	 * @return the pointer
	 * @throws IOException 
	 */
	public Pointer store(Allocation allocation, byte[] key, byte[] value, long ttl) throws IOException {
		Pointer pointer = new Pointer(this, allocation.metaOffset, key.length, value.length, ttl);
		underlyingStorage.put(allocation.metaOffset, makeItemBytes(allocation, pointer, key, value));
		// used storage update
		usedStorage.addAndGet(pointer.getItemSize() + Meta.META_SIZE);
		return pointer;
	}

	
	private byte[] makeItemBytes(Allocation allocation,Pointer pointer,byte[] key, byte[] value) {		
		byte[] bytes = new byte[Meta.META_SIZE+key.length+value.length];
		System.arraycopy(ByteUtil.toBytes(pointer.getLastAccessTime()), 0, bytes, Meta.LAST_ACCESS_OFFSET, 8);
		System.arraycopy(ByteUtil.toBytes(pointer.getTtl()), 0, bytes, Meta.TTL_OFFSET, 8);
		
		System.arraycopy(ByteUtil.toBytes(key.length), 0, bytes, Meta.KEY_SIZE_OFFSET, 4);
		System.arraycopy(ByteUtil.toBytes(value.length), 0, bytes, Meta.VALUE_SIZE_OFFSET, 4);

		System.arraycopy(key, 0, bytes, Meta.META_SIZE, key.length);
		System.arraycopy(value, 0, bytes, Meta.META_SIZE + key.length, value.length);
		return bytes;
	}

	@Override
	public Pointer store(byte[] key, byte[] value, long ttl) throws IOException {
		int payloadLength = key.length + value.length;
		Allocation allocation = allocate(payloadLength);
		if (allocation == null)
            return null; // not enough storage available

        return store(allocation, key, value, ttl);
	}

	/**
	 * Allocates storage for the payload, return null if not enough storage available.
	 *
	 * @param payloadLength the payload
	 * @return the allocation
	 */
	protected Allocation allocate(int payloadLength) {
		
		int itemOffset = currentItemOffset.addAndGet(Meta.META_SIZE+payloadLength);
		
		if(capacity < itemOffset){
			return null;
		}

        return new Allocation(itemOffset - payloadLength - Meta.META_SIZE);
	}


	@Override
	public byte[] retrieve(Pointer pointer) throws IOException {
		byte bytes[] = new byte[pointer.getValueSize()];
		underlyingStorage.get(pointer.getMetaOffset() + Meta.META_SIZE + pointer.getKeySize(), bytes);
		return bytes;
	}


	@Override
	public int markDirty(int dirtySize) {
		return dirtyStorage.addAndGet(dirtySize);
	}
	
	@Override
	public void close() throws IOException {
		if (underlyingStorage != null) {
			underlyingStorage.close();
		}
	}

	/**
	 * The Class Allocation.
	 */
	private static class Allocation {
		
		/** The item offset. */
		private int metaOffset;
	
		
		/**
		 * Instantiates a new allocation.
		 *
		 * @param itemOffset offset
		 * @param metaOffset offset
		 */
		public Allocation(int metaOffset) {
			this.metaOffset = metaOffset;
		}
	}
	
	@Override
	public long getDirty() {
		return dirtyStorage.get();
	}

	@Override
	public long getUsed() {
		return usedStorage.get();
	}

	@Override
	public long getCapacity() {
		return capacity;
	}

	@Override
	public double getDirtyRatio() {
		return (getDirty() * 1.0) / getCapacity();
	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public void free() {
		dirtyStorage.set(0);
		usedStorage.set(0);
		currentItemOffset.set(0); 
		underlyingStorage.free();
	}

	@Override
	public int compareTo(IBlock o) {
		if (getIndex() < o.getIndex()) {
			return -1;
		}else if (getIndex() == o.getIndex()) {
			return 0;
		}else {
			return 1;
		}
	}

	
	public Meta readMeta(int offset) throws IOException {
		Meta meta = new Meta(offset);
		//read meta
		byte[] bytes = new byte[Meta.META_SIZE];
		underlyingStorage.get(offset, bytes);
		byte[] l = new byte[8];
		byte[] n = new byte[4];
		System.arraycopy(bytes, Meta.LAST_ACCESS_OFFSET, l, 0, l.length);
		meta.setLastAccessTime(ByteUtil.ToLong(l));		
		System.arraycopy(bytes, Meta.TTL_OFFSET, l, 0, l.length);
		meta.setTtl(ByteUtil.ToLong(l));
		System.arraycopy(bytes, Meta.KEY_SIZE_OFFSET, n, 0, n.length);
		meta.setKeySize(ByteUtil.ToInt(n));
		System.arraycopy(bytes, Meta.VALUE_SIZE_OFFSET, n, 0, n.length);
		meta.setValueSize(ByteUtil.ToInt(n));
		return meta;				
	}

	@Override
	public List<Meta> getAllValidMeta() throws IOException {
		List<Meta> list = new ArrayList<Meta>();
		int useSize = 0;
		int offset = 0;
		for (; offset < capacity; ) {
			Meta meta = readMeta(offset);
			if(0==meta.getLastAccessTime()) {
				break;
			}
			if ((System.currentTimeMillis() - meta.getLastAccessTime()) < meta.getTtl()||meta.getTtl() == Meta.TTL_NEVER_EXPIRE) {
				list.add(meta);
				useSize += (meta.getKeySize()+meta.getValueSize()+Meta.META_SIZE);
			}
			offset += (Meta.META_SIZE+meta.getKeySize()+meta.getValueSize());
		}
		dirtyStorage.set(capacity - useSize);
		usedStorage.set(useSize);
		if(getDirtyRatio() < QuickCache.DEFAULT_DIRTY_RATIO_THRESHOLD) {
			list.clear();
		}
		return list;
	}

	@Override
	public Item readItem(Meta meta) throws IOException {
		Item item = new Item();
		//read item
		byte[] bytes = new byte[meta.getKeySize() + meta.getValueSize()];
		underlyingStorage.get(meta.getOffSet() + Meta.META_SIZE, bytes);		
		byte[] key = new byte[meta.getKeySize()];		
		System.arraycopy(bytes, 0, key, 0, key.length);
		byte[] value = new byte[meta.getValueSize()];		
		System.arraycopy(bytes, key.length, value, 0, value.length);		
		item.setKey(key);
		item.setValue(value);
		return item;
	}

}
