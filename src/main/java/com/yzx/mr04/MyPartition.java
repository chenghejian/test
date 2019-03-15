package com.yzx.mr04;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text k,Text v, int i) {
        String tou = k.toString();
        if(tou.contains("~~")){
            return 0;
        }else{
            return 1;
        }
    }
}
