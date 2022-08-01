package com.nuc.mapreduce.mapreduceDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WordDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop:9000");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordDriver.class);

        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WordBean.class);

        // 设置Shuffle阶段的分区类
        job.setPartitionerClass(WordPartitioner.class);

        // 设置Shuffle阶段的合并类
        job.setCombinerClass(WordCombiner.class);

        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(WordReducer.class);

        // 设置分区的个数 ---  reduceTask的个数
        job.setNumReduceTasks(2);
        // 设置辅助排序 --- 用于帮助reduceTask判断key是否相等 用于key分组
//        job.setGroupingComparatorClass(WordSortComparator.class);


        FileInputFormat.setInputPaths(job, new Path("/word.txt"));
        Path path = new Path("/output");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), configuration, "root");
        if(fs.exists(path)){
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 0 : 1);

    }
}
