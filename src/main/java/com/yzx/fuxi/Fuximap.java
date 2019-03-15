package com.yzx.fuxi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class Fuximap {

    public static class Mmap extends Mapper<LongWritable,Text,Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String va = value.toString();
            String[] split = va.split(",");
            for(String s:split){
                context.write(new Text(s),new IntWritable(1));
            }

        }
    }

    public static class MReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count =0 ;
            for(IntWritable in:values){
                int i=in.get();
                count+=i;
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJobName("count");
            job.setJarByClass(Fuximap.class);
            job.setMapperClass(Mmap.class);
            job.setReducerClass(MReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,new Path("D:\\杂物间\\fuxi"));
            job.setOutputFormatClass(TextOutputFormat.class);
            Path p = new Path("D:\\杂物间\\fuxi\\f1");
            FileSystem fs = p.getFileSystem(conf);
            if(fs.exists(p)){
                fs.delete(p);
            }
            FileOutputFormat.setOutputPath(job,p);
            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


   }

