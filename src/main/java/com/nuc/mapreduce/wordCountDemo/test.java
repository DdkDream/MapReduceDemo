package com.nuc.mapreduce.wordCountDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class test {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop:9000");

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop:9000"), conf, "root");
        System.out.println(fileSystem);

    }
}
