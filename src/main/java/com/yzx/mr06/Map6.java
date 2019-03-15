package com.yzx.mr06;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
@SuppressWarnings("all")
public class Map6 {

    public static class MyMap6 extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String row = value.toString();
            String[] split = row.split("\t");
            String diseaseid = split[2];
            String hospitalid  = split[1];
            context.write(new Text("hos"+hospitalid), new Text(row));
            context.write(new Text("dis"+diseaseid), new Text(row));
            context.write(new Text("hdis"+hospitalid+"~~~"+diseaseid), new Text(row));
        }
    }

}
