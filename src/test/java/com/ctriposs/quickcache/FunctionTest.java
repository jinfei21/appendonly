package com.ctriposs.quickcache;

import java.io.IOException;

import junit.framework.TestCase;

import com.ctriposs.quickcache.CacheConfig.StartMode;
import com.ctriposs.quickcache.CacheConfig.StorageMode;

public class FunctionTest extends TestCase {

	public static void main(String args[]) {

		try {
			CacheConfig config = new CacheConfig();
			config.setStorageMode(StorageMode.PureFile);
			config.setStartMode(StartMode.RecoveryFromFile);
			QuickCache<String> cache = new QuickCache<String>("D:\\data", config);
			
			String str = "123";
			byte res[] = cache.get("2");
			System.out.println("get2:"+new String(res));
			for(int i=0;i<10;i++) {
				cache.put("2", ("hello"+i).getBytes());
			}
			
			res = cache.get("2");
			System.out.println("get2:"+new String(res));
			
			

			cache.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
