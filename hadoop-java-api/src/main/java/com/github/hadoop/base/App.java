/**
 * 
 */
package com.github.hadoop.base;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

/**
 * HADOOP JAVA API
 * @author black
 *
 */
public class App {
	/**
	 * 注册协议的处理器工厂，让Java程序识别hdfs协议
	 */
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String host = "hdfs://master:12306";  //unknown protocol: hdfs
		String fileName = host + "/home/data/help.txt";
		
		URL url = new URL(fileName);
		URLConnection urlConnection = url.openConnection();
		InputStream in = urlConnection.getInputStream();
		//获取到文件，保存到本地磁盘上
		FileOutputStream out = new FileOutputStream("help.txt");
		byte[] buf = new byte[1024];
		int lenth = -1;
		while (-1 != (lenth = in.read(buf))) {
			out.write(buf, 0, lenth);
		}
		in.close();
		out.close();
		System.out.println("ok!!!");
	}

}
