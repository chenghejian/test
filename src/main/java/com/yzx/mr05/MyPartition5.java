package com.yzx.mr05;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartition5 extends Partitioner<Text,Text> {

    @Override
    public int getPartition(Text k,Text v, int i) {
        String tou = k.toString();
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
