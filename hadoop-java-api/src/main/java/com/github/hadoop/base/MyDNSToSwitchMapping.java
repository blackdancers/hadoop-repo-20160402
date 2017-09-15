package com.github.hadoop.base;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.net.DNSToSwitchMapping;

/**
 * 机架感知 rack-aware
 * 
 * @author black
 *
 */
public class MyDNSToSwitchMapping implements DNSToSwitchMapping {
	
	/**
	 * @param names  
	 * 	传递客户端的IP地址列表，返回机架感知的路径列表
	 *  /rack1/slave01
	 *  /rack1/slave02
	 *  /rack2/slave03
	 */
	@Override
	public List<String> resolve(List<String> names) {
		try {
			FileOutputStream fos = new FileOutputStream("/home/ecmgr/dns.txt");
			for (String name : names) {
				fos.write((name+"\r\n").getBytes());
			}
			fos.close();
			
			List<String> pathRackList = new ArrayList<>();
			if (CollectionUtils.isNotEmpty(names)) {
				for (String name : names) {
					if (name.startsWith("slave0")) {
						//主机名称
					}else if (name.startsWith("192")) {
						//客户端IP
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void reloadCachedMappings() {
		// TODO Auto-generated method stub

	}

	@Override
	public void reloadCachedMappings(List<String> names) {
		// TODO Auto-generated method stub

	}

}
