package com.nuc.mapreduce.mapreduceDemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartitioner extends Partitioner<Text, WordBean> {
    @Override
    public int getPartition(Text text, WordBean wordBean, int i) {
        String word = text.toString();
        char[] chars = word.toCharArray();
        if(chars[0] <= 90 && chars[0] >= 65){
            return 1;
        }else{
            return 0;
        }
    }
}
