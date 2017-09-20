package com.github.hadoop.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import com.hadoop.compression.lzo.LzoCodec;

public class SequenceFileTest {

	
	@Test
	public void write() throws Exception {
		Configuration conf = new Configuration();
		//conf.set("", "");
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/seq.file");
		Writer writer = SequenceFile.createWriter(fileSystem, conf, path, IntWritable.class, Text.class);
		for (int i = 1; i < 101; i++) {
			writer.append(new IntWritable(i), new Text("jack"+i));
		}
		writer.close();
		System.out.println("over..");
	}
	
	/**
	 * 自定义设置同步点
	 * @throws Exception
	 */
	@Test
	public void writeWithSync() throws Exception {
		Configuration conf = new Configuration();
		//conf.set("", "");
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/seq.file");
		Writer writer = SequenceFile.createWriter(fileSystem, conf, path, IntWritable.class, Text.class);
		for (int i = 1; i < 101; i++) {
			if (i %5==0) {
				writer.sync();//创建同步点
			}
			writer.append(new IntWritable(i), new Text("jack"+i));
		}
		writer.close();
		System.out.println("over..");
	}
	
	/**
	 * 压缩写入
	 * @throws Exception
	 */
	@Test
	public void writeWithCodec() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/mySeq2.file");
		Writer writer = SequenceFile.createWriter(fs, conf, path, 
				IntWritable.class, Text.class,CompressionType.BLOCK,new DeflateCodec());
		for (int i = 1; i < 101; i++) {
			writer.append(new IntWritable(i), new Text("jack"+i));
		}
		writer.close();
	}
	
	/**
	 * 读取seq文件
	 * @throws Exception
	 */
	@Test
	public void readSeqFile() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/seq.file");
		Reader reader = new SequenceFile.Reader(fileSystem,path,conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		while (reader.next(key,value)) {
			System.out.println(key+"=="+value);
		}
		System.out.println("over..");
	}
	
	@Test
	public void readSeqDemo() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/seq.file");
		Reader reader = new SequenceFile.Reader(fileSystem,path,conf);
//		IntWritable key = new IntWritable();
//		Text value = new Text();
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
//		while (reader.next(key,value)) {
//			System.out.println(key+"=="+value);
//		}
//		reader.seek(180);
//		reader.next(key, value);
//		System.out.println(key+":"+value);
		int syncPos = 300;
		reader.sync(syncPos);//定位下一个同步点
		reader.next(key, value);
		System.out.println(syncPos+" -> "+reader.getPosition()+" -> "+key+":"+value);
		
		
		
		IOUtils.closeStream(reader);
		System.out.println("over..");
	}
	
	/**
	 * 块位置
	 * @throws Exception
	 */
	@Test
	public void readSeqPosition() throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/mySeq2.file");
		Reader reader = new SequenceFile.Reader(fileSystem,path,conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		
		CompressionType compressionType = reader.getCompressionType();
		CompressionCodec codec = reader.getCompressionCodec();
		System.out.println(compressionType);
		System.out.println(codec);
		
		//当前key对应的字节数
		long position = reader.getPosition();
		
		while (reader.next(key,value)) {
			System.out.println(position+": "+key+"=="+value);
			//取出下一个位置
			position = reader.getPosition();
		}
		IOUtils.closeStream(reader);
		System.out.println("over..");
	}
	
	
}
