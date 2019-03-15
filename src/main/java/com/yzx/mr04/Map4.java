package com.yzx.mr04;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map4 {

    public static class MyMap4 extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String row = value.toString();
                String[] split = row.split("\t");
                String diseaseid = split[2];
                String hospitalid  = split[1];
                context.write(new Text(diseaseid+"\t"), new Text(row));
                context.write(new Text(hospitalid+"~~~"+diseaseid+"\t"), new Text(row));
        }
    }

}
