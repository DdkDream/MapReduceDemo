package com.nuc.mapreduce.mapreduceDemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordReducer extends Reducer<Text, WordBean, NullWritable, WordBean> {

    @Override
    protected void reduce(Text key, Iterable<WordBean> values, Reducer<Text, WordBean, NullWritable, WordBean>.Context context) throws IOException, InterruptedException {
        WordBean sumWordBean = new WordBean();
        for (WordBean value : values) {
            sumWordBean.setSum(sumWordBean.getSum() + value.getSum());
        }

        context.write(NullWritable.get(), sumWordBean);
    }
}
