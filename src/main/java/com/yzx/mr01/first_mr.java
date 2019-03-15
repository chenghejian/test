package com.yzx.mr01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class first_mr {

    public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if ("record.txt".equals(fileName)) {
                String[] split = value.toString().split("\t");
                String idd = split[0];
                int i = idd.length();
                String houu = value.toString().substring(i);
                Text id = new Text(idd);
                Text hou = new Text(houu + "!@#$%^");
                context.write(id, hou);

            } else if ("reimburse.txt".equals(fileName)) {
                String[] split = value.toString().split(",");
                String idd = split[0];
                int i = idd.length();
                String houu = value.toString().substring(i);
                Text id = new Text(idd);
                Text hou = new Text(houu);
                context.write(id, hou);
            }
        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String tou = key.toString();
            String wei1 = null;
            String wei2 = null;
            double money = 0;
            for (Text value : values) {
                String hou = value.toString();
                if (hou.contains("!@#$%^")) {
                    wei1 = hou;
                } else {
                    String[] split = hou.split(",");
                    wei2 = split[1];
                    String moneyy = split[split.length - 1];
                    double v = Double.parseDouble(moneyy);
                    money += v;

                }
            }
            context.write(new Text(tou + wei1.substring(0, wei1.length() - 3) + "\t" + wei2 + "\t" + String.valueOf(money)), NullWritable.get());


        }
    }

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(first_mr.class);
            job.setJobName("The_first");
            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("D:\\杂物间\\智能诊导"));
            job.setOutputFormatClass(TextOutputFormat.class);
            Path p = new Path("D:\\杂物间\\智能诊导\\f1");
            FileSystem fs = p.getFileSystem(conf);
            if (fs.exists(p)) {
                fs.delete(p);
            }
            FileOutputFormat.setOutputPath(job, p);
            boolean waitForCompletion = job.waitForCompletion(true);
            System.exit(waitForCompletion ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
