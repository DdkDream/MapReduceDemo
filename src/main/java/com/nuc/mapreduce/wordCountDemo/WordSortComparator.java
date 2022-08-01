package com.nuc.mapreduce.wordCountDemo;

import org.apache.hadoop.io.WritableComparator;

import java.util.Objects;

public class WordSortComparator extends WritableComparator {

    public WordSortComparator() {
    }

    @Override
    public int compare(Object a, Object b) {
        WordBean wordBean1 = (WordBean) a;
        WordBean wordBean2 = (WordBean) b;

        if(wordBean1.getWord().equals(wordBean2.getWord()) && Objects.equals(wordBean1.getSum(), wordBean2.getSum())){
            return 0;
        }else{
            return 1;
        }
    }
}
