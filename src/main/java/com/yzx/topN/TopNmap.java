package com.yzx.topN;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNmap {

    public static class Tmap extends Mapper<LongWritable, Text,Text, TBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(" ");
            String province = s[0];
            String city = s[1];
            Integer grade =Integer.parseInt(s[2]);
            TBean tb = new TBean(province,city,grade);
            context.write(new Text(province),tb);

        }
    }
}
