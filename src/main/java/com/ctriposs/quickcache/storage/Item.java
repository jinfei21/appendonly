package com.ctriposs.quickcache.storage;

public class Item {
	
	private Meta meta;
	private byte[] key;
	private byte[] value;
	
	public Item(byte[] key,byte[] value) {
		this.key = key;
		this.value = value;
		this.meta = null;
	}
	
	public Item(Meta meta) {
		this.key = null;
		this.value = null;
		this.meta = meta;
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

	public Meta getMeta() {
		return meta;
	}
}
