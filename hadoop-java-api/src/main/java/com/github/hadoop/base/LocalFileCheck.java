package com.github.hadoop.base;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * http://www.itwendao.com/article/detail/392679.html
 */
public class LocalFileCheck {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String filePath = "file:///home/ecmgr/yum.info";
        FileSystem fileSystem = new LocalFileSystem(new RawLocalFileSystem());
        fileSystem.initialize(URI.create(filePath), conf);
        FSDataOutputStream fout = fileSystem.create(new Path(filePath), new Progressable() {
            @Override
            public void progress() {
                System.out.println(" . ");
            }
        });
        for (int i = 0; i < 10; i++) {
            fout.write(i);
        }
        fout.close();
        fileSystem.close();
    }

}
