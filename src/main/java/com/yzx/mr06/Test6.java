package com.yzx.mr06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Test6 {

    public static void main(String[] args) {


        {
            {
                Configuration conf = new Configuration();
                try {
                    Job job = Job.getInstance(conf);
                    job.setJarByClass(Test6.class);
                    job.setJobName("NO.6");
                    job.setMapperClass(Map6.MyMap6.class);
                    job.setReducerClass(Reduce6.MyReduce6.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    //指定partitioner
                    job.setPartitionerClass(MyPartition6.class);
                    //指定partitioner分区数量
                    job.setNumReduceTasks(3);
                    job.setInputFormatClass(TextInputFormat.class);
                    FileInputFormat.setInputPaths(job, new Path("D:\\杂物间\\智能诊导\\f1"));
                    job.setOutputFormatClass(TextOutputFormat.class);
                    Path p = new Path("D:\\杂物间\\智能诊导\\f6");
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
