package com.nuc.mapreduce.wordCountDemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCombiner extends Reducer<Text, WordBean, Text, WordBean> {

    @Override
    protected void reduce(Text key, Iterable<WordBean> values, Reducer<Text, WordBean, Text, WordBean>.Context context) throws IOException, InterruptedException {
        WordBean wordBean = new WordBean();
        wordBean.setWord(key.toString());
        Long sum = 0L;
        for (WordBean value : values) {
            sum += value.getSum();
        }
        wordBean.setSum(sum);
        context.write(new Text(wordBean.getWord()), wordBean);
    }
}
