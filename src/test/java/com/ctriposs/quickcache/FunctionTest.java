package com.ctriposs.quickcache;

import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;

import com.ctriposs.quickcache.CacheConfig.StartMode;
import com.ctriposs.quickcache.CacheConfig.StorageMode;

public class FunctionTest extends TestCase {

	public static void main(String args[]) {

		try {
			CacheConfig config = new CacheConfig();
			config.setStorageMode(StorageMode.PureFile);
			config.setStartMode(StartMode.RecoveryFromFile);
			final QuickCache<String> cache = new QuickCache<String>("D:\\data",
					config);
			final Random rand = new Random();
			final String str="fdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvn!@#@!$#^$%&%&^%&%^&%$$@#$@#$%^#$^$#2342314124123412412fdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvnfdsafsadqwewqiieurweiqusdfxmvn";
			byte[] data = str.getBytes();
			int i = 0;
			final int count = 1000*1000;
			for (; i < count; i++) {
				cache.put("" + i, data);
			}
			System.out.println("put end");
			new Thread() {
				public void run() {
					System.out.println("get start");
					for (int i = 0; i < count; i++) {

						try {
							byte[] res = cache.get("" + i);

							if (res != null) {
								String s = new String(res);
								if (!str.equals(s)) {
									System.out.println(s);
								}
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			}.start();

			new Thread() {
				public void run() {
					System.out.println("delete start");
					for (int i = 0; i < count; i++) {
						if (rand.nextDouble() < 0.5) {
							try {
								cache.delete("" + i);
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}.start();

			cache.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while(true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
