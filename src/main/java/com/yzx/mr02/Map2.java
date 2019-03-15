package com.yzx.mr02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map2 {

    public static class MyMap2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String row = value.toString();
            String[] split = row.split("\t");
            if("1".equals(split[split.length - 7])){
                String hospitalId = split[1];
                context.write(new Text(hospitalId), new Text(row));

            }
        }
    }
}
