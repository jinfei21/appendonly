package com.ctriposs.quickcache.storage;

public class Item {
	
	private byte[] key;
	private byte[] value;
	
	public Item(byte[] key,byte[] value) {
		this.key = key;
		this.value = value;
	}
	
	public Item() {
		this.key = null;
		this.value = null;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}

	public byte[] getValue() {
		return value;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

}
