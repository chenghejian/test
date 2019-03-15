package com.yzx.topN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TopTest {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        try {
//            conf.setInt("top.n",5);
            conf.addResource("top.xml");
            Job job = Job.getInstance(conf);
            job.setJobName("topn");
            job.setJarByClass(TopTest.class);
            job.setMapperClass(TopNmap.Tmap.class);
            job.setReducerClass(TopNreduce.Treduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(TBean.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,new Path("D:\\杂物间\\top"));
            job.setOutputFormatClass(TextOutputFormat.class);
            Path path = new Path("D:\\杂物间\\top\\f1");
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)){
                fs.delete(path);
            }
            FileOutputFormat.setOutputPath(job,path);
            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1 );

        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
