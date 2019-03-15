package com.yzx.mr03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Test3 {

    public static void main(String[] args) {

        {

            Configuration conf = new Configuration();
            try {
                Job job = Job.getInstance(conf);
                job.setJarByClass(Test3.class);
                job.setJobName("The_3rd");
                job.setMapperClass(Map3.MyMap3.class);
                job.setReducerClass(Reduce3.MyReduce3.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.setInputPaths(job, new Path("D:\\杂物间\\智能诊导\\f1"));
                job.setOutputFormatClass(TextOutputFormat.class);
                Path p = new Path("D:\\杂物间\\智能诊导\\f3");
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

}
