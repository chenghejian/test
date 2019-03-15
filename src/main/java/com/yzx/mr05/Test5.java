package com.yzx.mr05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Test5 {

    public static void main(String[] args) {

        {
            {
                Configuration conf = new Configuration();
                try {
                    Job job = Job.getInstance(conf);
                    job.setJarByClass(Test5.class);
                    job.setJobName("The_3rd");
                    job.setMapperClass(Map5.MyMap5.class);
                    job.setReducerClass(Reduce5.MyReduce5.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    //指定partitioner
                    job.setPartitionerClass(MyPartition5.class);
                    //指定partitioner分区数量
                    job.setNumReduceTasks(3);
                    job.setInputFormatClass(TextInputFormat.class);
                    FileInputFormat.setInputPaths(job, new Path("D:\\杂物间\\智能诊导\\f1"));
                    job.setOutputFormatClass(TextOutputFormat.class);
                    Path p = new Path("D:\\杂物间\\智能诊导\\f5");
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

}
