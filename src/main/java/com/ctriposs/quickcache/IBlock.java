package com.ctriposs.quickcache;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.ctriposs.quickcache.storage.Item;
import com.ctriposs.quickcache.storage.Meta;
import com.ctriposs.quickcache.storage.Pointer;

public interface IBlock extends Comparable<IBlock>, Closeable {
	  /**
     * Retrieves the payload associated with the pointer and always update the access time.
     *
     * @param pointer the pointer
     * @return the byte[]
     * @throws IOException
     */
    byte[] retrieve(Pointer pointer) throws IOException;

	
	/**
	 * Stores the payload.
	 *
	 * @param key the key
     * @param value the data
     * @param ttl time-to-live
	 * @return the pointer
	 * @throws IOException
	 */
	Pointer store(byte[] key, byte[] value, long ttl) throws IOException;
	
	/**
	 *  Marks exSpace as dirty.
	 * 
	 * @param dirty size
	 * @return total dirty size
	 */
	int markDirty(int dirtySzie);
	
	/**
	 * Get item for this meta
	 * 
	 * @return item
	 */
	Item readItem(int offset) throws IOException; 
	
	/**
	 * Calculates and returns total size of the dirty space.
	 *
	 * @return the total size of the dirty space.
	 */
	long getDirty();
	
	/**
	 * Calculates and returns total size of the used space.
	 * 
	 * @return the total size of the used space.
	 */
	long getUsed();
	
	/**
	 * Calculates and returns total capacity of the block.
	 *
	 * @return the total capacity of the block.
	 */
	long getCapacity();
	
	/**
	 * Calculates and returns the dirty to capacity ratio
	 *  
	 * @return dirty ratio
	 */
	double getDirtyRatio();
	
	/**
	 * Get the index of this storage block
	 * 
	 * @return an index
	 */
	int getIndex();
	
	/**
	 * Frees the storage.
	 */
	void free();
	
}
