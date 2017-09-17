package com.github.hadoop.codec;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

/**
 * 
 * @author ecmgr
 *
 */
public class CodecTest {

	@Test
	public void compress() throws Exception {
		String codecClassName = "org.apache.hadoop.io.compress.DefaultCodec";
		Class<?> clazz = Class.forName(codecClassName);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz, conf);
		
		FileInputStream fis = new FileInputStream("/data/soft/VM.tar.gz");
		FileOutputStream fos = new FileOutputStream("/home/ecmgr/VM.deflate");
		
		CompressionOutputStream cos = codec.createOutputStream(fos);
		IOUtils.copyBytes(fis, cos, 1024);
		cos.finish();
		cos.close();
		fos.close();
		fis.close();
		System.out.println("ok");
	}
	
	@Test
	public void deCompress() throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		Class<?> codecClazz = DeflateCodec.class;
		DeflateCodec codec = (DeflateCodec) ReflectionUtils.newInstance(codecClazz, conf);
		CompressionInputStream cis = codec.createInputStream(new FileInputStream("/home/ecmgr/VM.deflate"));
		FileOutputStream fos = new FileOutputStream("/home/ecmgr/new_VM.tar.gz");
		IOUtils.copyBytes(cis, fos, conf);
		cis.close();
		fos.close();
		System.out.println("ok");
	} 

}




















