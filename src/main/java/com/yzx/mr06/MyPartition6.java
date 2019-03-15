package com.yzx.mr06;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
@SuppressWarnings("all")
public class MyPartition6 extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text key,Text value, int i) {
        String tou = key.toString();
        if(tou.startsWith("hos")){
            return 0;
        }else if(tou.startsWith("dis")){
            return 1;
        }else if(tou.startsWith("hdis")){
            return 2;
        }else{
            return 3;
        }


    }
}
