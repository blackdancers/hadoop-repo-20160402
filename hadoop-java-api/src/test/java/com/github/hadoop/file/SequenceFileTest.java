package com.github.hadoop.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class SequenceFileTest {

	
	@Test
	public void write() throws Exception {
		Configuration conf = new Configuration();
		//conf.set("", "");
		FileSystem fileSystem = FileSystem.get(conf);
		Path path = new Path("/home/ecmgr/seq.file");
		Writer writer = SequenceFile.createWriter(fileSystem, conf, path, IntWritable.class, Text.class);
		writer.append(new IntWritable(1), new Text("jack"));
		writer.append(new IntWritable(1), new Text("jack"));
		writer.append(new IntWritable(1), new Text("jack"));
		writer.append(new IntWritable(1), new Text("jack"));
		writer.append(new IntWritable(1), new Text("jack"));
		writer.close();
		System.out.println("over..");
	}
}
