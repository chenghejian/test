package com.yzx.mr03;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map3 {

    public static class MyMap3 extends Mapper<LongWritable,Text, Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String row = value.toString();
            String[] split = row.split("\t");
                String hospitalId = split[1];
                context.write(new Text(hospitalId), new Text(row));
            }
    }
}
