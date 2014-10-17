package com.ctriposs.quickcache.storage;

import java.io.Serializable;
import java.util.Arrays;

import com.ctriposs.quickcache.utils.ByteUtil;
import com.ctriposs.quickcache.utils.HashUtil;

public class WrapperKey implements Serializable,Comparable {

	private static final long serialVersionUID = 1L;
	private byte[] key;
	private int hashCode;

	public WrapperKey(byte[] key) {
		this.key = key;
		this.hashCode = HashUtil.JSHash(key);
	}
	
	@Override
	public boolean equals(Object obj) {

		if (obj instanceof WrapperKey) {
			WrapperKey other = (WrapperKey) obj;
			return Arrays.equals(key, other.key);
		}
		return false;
	}

	public byte[] getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public int compareTo(Object obj) {
		if (obj instanceof WrapperKey) {
			WrapperKey other = (WrapperKey) obj;
			return ByteUtil.compare(key, other.key);
		}
		return 1;
	}
}
