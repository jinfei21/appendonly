package com.ctriposs.quickcache.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.ctriposs.quickcache.IStorage;

public class PureFileStorage implements IStorage {

	private FileChannel fileChannel;
	private RandomAccessFile raf;
	
	public PureFileStorage(String dir, int index, int capacity) throws IOException {
		File dirFile = new File(dir);
		if (!dirFile.exists()) {
            dirFile.mkdirs();
        }

		String fullFileName = dir + index + "-" + System.currentTimeMillis() + DATA_FILE_SUFFIX;
		raf = new RandomAccessFile(fullFileName, "rw");
		raf.setLength(capacity);
		fileChannel = raf.getChannel();
	}

	public PureFileStorage(File file, int capacity) throws IOException {
		raf = new RandomAccessFile(file, "rw");
		raf.setLength(capacity);
		fileChannel = raf.getChannel();
	}
	
	@Override
	public void get(int position, byte[] dest) throws IOException {
		fileChannel.read(ByteBuffer.wrap(dest), position);
	}

	@Override
	public void put(int position, byte[] source) throws IOException {
		fileChannel.write(ByteBuffer.wrap(source), position);
	}

	@Override
	public void free() {
		// nothing to do here
		try {
			fileChannel.truncate(0);
		} catch (IOException e) {
		}
	}

	@Override
	public void close() throws IOException {
		if (this.fileChannel != null) {
			this.fileChannel.close();
		}
		if (this.raf != null) {
			this.raf.close();
		}
	}
}
