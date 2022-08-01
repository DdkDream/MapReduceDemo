package com.nuc.mapreduce.wordCountDemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, WordBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arrays = line.split(" ");
        for (String array : arrays) {
            Long sum = 1L;
            WordBean wordBean = new WordBean();
            wordBean.setWord(array);
            wordBean.setSum(sum);
            context.write(new Text(array), wordBean);
        }
    }
}
