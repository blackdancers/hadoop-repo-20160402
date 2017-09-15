package com.github.hadoop.base;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsChecksum {

	
private FileSystem fs;
	
	@Before
	public void initConf() {
		//加载配置文件，需要手动指定，不然会使用默认jar包中的core-site.xml文件
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "file:///");
		try {
			fs =FileSystem.get(conf);
			URI defaultUri = FileSystem.getDefaultUri(conf);
			System.err.println(defaultUri.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void forChecksum() throws Exception  {
		Path path = new Path("/home/ecmgr/help.txt");
		try {
			FSDataOutputStream fos = fs.create(path);
			fos.write("hello".getBytes());
			fos.close();
		} finally {
			fs.close();
			System.out.println("ok");
		}
	}
	
	
	
}
