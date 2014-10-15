package com.ctriposs.quickcache;

import java.io.Closeable;
import java.io.IOException;

/**
 * The Interface ICache.
 *
 * @param <K> the key type
 */
public interface ICache<K> extends Closeable {
	
	/**
	 * Puts the value with the specified key.
	 *
	 * @param key the key
	 * @param value the value
	 * @throws IOException
	 */
	void put(K key, byte[] value) throws IOException;
	
	/**
	 * Puts the value with specified key and time to idle in milliseconds.
	 * 
	 * @param key the key
	 * @param value the value
	 * @param ttl the time to idle value in milliseconds
	 * @throws IOException
	 */
	void put(K key, byte[] value, long ttl)  throws IOException;

	/**
	 * Gets the value with the specified key.
	 *
	 * @param key the key
	 * @return the value
	 * @throws IOException
	 */
	byte[] get(K key) throws IOException;

	/**
	 * Delete the value with the specified key.
	 *
	 * @param key the key
	 * @return the value
	 * @throws IOException
	 */
	byte[] delete(K key) throws IOException;
	
	/**
	 * Check if Cache contains the specified key.
	 *
	 * @param key the key
	 * @return true, if successful
	 */
	boolean contains(K key) throws IOException;

	/**
	 * Clear the cache.
	 */
	void clear();
	
	/**
	 * Calculates the Hit ratio.
	 *
	 * @return the double
	 */
	double hitRatio();
}
