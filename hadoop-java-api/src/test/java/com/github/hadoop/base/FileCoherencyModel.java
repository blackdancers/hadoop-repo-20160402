package com.github.hadoop.base;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * 文件系统的一致性(Coherency Model)
 *
 */
public class FileCoherencyModel {
	
	private FileSystem fs;

	@Before
	public void initConf() {
		// 加载配置文件，需要手动指定，不然会使用默认jar包中的core-site.xml文件
		Configuration conf = new Configuration();
		try {
			fs = FileSystem.get(conf);
			URI defaultUri = FileSystem.getDefaultUri(conf);
			System.err.println(defaultUri.toString());// hdfs://master:8020
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void writeFile() throws IOException {
		Path path = new Path("hdfs://master:8020/home/data/api-test.txt");
		FSDataOutputStream fos = fs.create(path);
		fos.write("Hello".getBytes("UTF-8"));
		fos.hflush(); //Flush out the data in client's user buffer清理客户端缓冲数据，被其他客户端可见
		fos.write("world".getBytes("UTF-8"));
		fos.hsync();//  flush out the data in client's user buffer all the way to the disk device
		//清理客户端缓冲数据，并写入磁盘，其他客户端不能立即可见
		fos.close();
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
