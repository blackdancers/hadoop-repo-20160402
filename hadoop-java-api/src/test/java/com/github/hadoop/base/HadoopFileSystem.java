package com.github.hadoop.base;

import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * 使用Hadoop的FileSystem实现文件读取
 * 
 * 1,Configuration  加载配置文件
 * 2,Path 区分绝对路径和相对路径
 * 3,FileSystem
 * 		org.apache.hadoop.hdfs.DistributedFileSystem  用于和Hadoop的HDFS交互的API
 * 
 static{
    //print deprecation warning if hadoop-site.xml is found in classpath
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    if(cL.getResource("hadoop-site.xml")!=null) {
      LOG.warn("DEPRECATED: hadoop-site.xml found in the classpath. Usage of hadoop-site.xml is deprecated. Instead use core-site.xml, "
          + "mapred-site.xml and hdfs-site.xml to override properties of core-default.xml, mapred-default.xml and hdfs-default.xml respectively");
    }
    
    // Add a default resource. Resources are loaded in the order of the resources added.
    // @param name file name. File should be present in the classpath.
    addDefaultResource("core-default.xml");
    addDefaultResource("core-site.xml");
  }
  
 *
 */
public class HadoopFileSystem {
	
	private FileSystem fs;
	
	@Before
	public void initConf() {
		//加载配置文件，需要手动指定，不然会使用默认jar包中的core-site.xml文件
		Configuration conf = new Configuration();
		try {
			fs =FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void writeFile() throws IOException {
		Path path = new Path("hdfs://master:12306/home/data/api-test.txt");
		FSDataOutputStream fsDataOutputStream = fs.create(path);//装饰流
		fsDataOutputStream.write("使用Hadoop的FileSystem实现文件读取".getBytes("UTF-8"));
		fsDataOutputStream.flush();
		fsDataOutputStream.close();
		fs.close();
	}
	
	/**
	 * 指定文件副本数
	 * @throws IOException
	 */
	@Test
	public void writeFileInReplication() throws IOException {
		Path path = new Path("hdfs://master:12306/home/data/replication.txt");
		FSDataOutputStream fsDataOutputStream = fs.create(path,(short) 2);//装饰流
		fsDataOutputStream.write("指定文件副本数".getBytes("UTF-8"));
		fsDataOutputStream.flush();
		fsDataOutputStream.close();
		fs.close();
	}
	
	@Test
	public void readFile() throws IOException {
		Path path = new Path("/home/data/api-test.txt");
		FSDataInputStream fsDataInputStream = fs.open(path);
		FileOutputStream out = new FileOutputStream("D:/temp_files/api-test.txt");
		
		
		IOUtils.copyBytes(fsDataInputStream, out, 1024);
		IOUtils.closeStream(fsDataInputStream);
		IOUtils.closeStream(out);
		System.out.println("ok.");
		
	}
	
	/**
	 * 文件定位
	 * @throws IOException
	 */
	@Test
	public void readFileSeek0() throws IOException {
		Path path = new Path("/home/data/jdk-8u141-linux-x64.tar.gz");
		FSDataInputStream fsDataInputStream = fs.open(path);
		//读取第一块的内容
		//long count = 134217728;
		FileOutputStream out = new FileOutputStream("D:/temp_files/jdk-8u141-linux-x64.tar.gz1");
		byte[] buf = new byte[1024];
		for (int i = 0; i < 128 * 1024; i++) {
			fsDataInputStream.read(buf);
			out.write(buf);
		}
		
		IOUtils.closeStream(fsDataInputStream);
		IOUtils.closeStream(out);
		System.out.println("ok.");
		
	}
	
	/**
	 * 文件定位
	 * @throws IOException
	 */
	@Test
	public void readFileSeek128() throws IOException {
		Path path = new Path("/home/data/jdk-8u141-linux-x64.tar.gz");
		FSDataInputStream fsDataInputStream = fs.open(path);
		/**
		 * Seek to the given offset.
		 */
		fsDataInputStream.seek(134217728);//1024*1024*8 = 128M  读取第二块 51298777
		//读取第一块的内容
		
		FileOutputStream out = new FileOutputStream("D:/temp_files/jdk-8u141-linux-x64.tar.gz2");
		
		
		IOUtils.copyBytes(fsDataInputStream, out, 1024);
		IOUtils.closeStream(fsDataInputStream);
		IOUtils.closeStream(out);
		System.out.println("ok.");
		
	}
	/**
	 * 创建目录
	 * @throws IOException
	 */
	@Test
	public void mkdir() throws IOException {
		Path path = new Path("/home/data/temp");
		 /**
		   * Construct by the given {@link FsAction}.
		   * @param u user action
		   * @param g group action
		   * @param o other action
		   * 赋予权限
		   */
		FsPermission permission = new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
		boolean ok = fs.mkdirs(path, permission);
		System.out.println(FsPermission.getDirDefault());
		System.out.println(FsPermission.getDefault());
		System.out.println(ok);
		
	}
	
	/**
	 * 文件状态
	 * @throws IOException
	 */
	@Test
	public void showFileStatus() throws IOException {
		Path path = new Path("/home/data/jdk-8u141-linux-x64.tar.gz");
		FileStatus fileStatus = fs.getFileStatus(path);
		Class<FileStatus> clazz = FileStatus.class;
		Method[] methods = clazz.getDeclaredMethods();
		for (Method method : methods) {
			String name = method.getName();
			Class<?>[] types = method.getParameterTypes();
			if (name.startsWith("get") && (types == null || types.length == 0)) {
				if (!name.equals("getSymlink")) {
					try {
						Object invoke = method.invoke(fileStatus, null);
						System.out.println(name + "() = " + invoke);
					} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						e.printStackTrace();
					}
				}
				
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
