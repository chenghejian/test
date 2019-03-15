package com.yzx.mr07;

import com.yzx.mr01.first_mr;
import com.yzx.mr06.Map6;
import com.yzx.mr06.MyPartition6;
import com.yzx.mr06.Reduce6;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Test7 {

    static JobConf conf = new JobConf();

    public static Job getJob1() {
        Job job = null;
        try {
            job = Job.getInstance(conf);
            job.setJarByClass(Test7.class);
            job.setJobName("jpb1");
            job.setMapperClass(first_mr.MyMap.class);
            job.setReducerClass(first_mr.MyReduce.class);
            //map输出
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //reduce输出
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            //map输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("D:\\杂物间\\智能诊导"));
            //指定输出类型和地址
            job.setOutputFormatClass(TextOutputFormat.class);
            Path p = new Path("D:\\杂物间\\智能诊导\\f1");
            FileSystem fs = p.getFileSystem(conf);
            if (fs.exists(p)) {
                fs.delete(p);
            }
            FileOutputFormat.setOutputPath(job, p);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public static Job getJob2() {
        Job job = null;
        try {
            job = Job.getInstance(conf);
            job.setJarByClass(Test7.class);
            job.setJobName("jpb2");
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public static void main(String[] args) {

        try {
            //子控制器1
            ControlledJob controlledJob1 = new ControlledJob(conf);
            //把job1放入子控制器1
            controlledJob1.setJob(getJob1());
            //子控制器2
            ControlledJob controlledJob2 = new ControlledJob(conf);
            //把job1放入子控制器2
            controlledJob2.setJob(getJob2());
            //因为 job2是依赖于job1的结果，所以需要添加依赖关系
            controlledJob2.addDependingJob(controlledJob1);
            //把子控制器放入主控制器中
            JobControl jobControl = new JobControl("???");
            jobControl.addJob(controlledJob1);
            jobControl.addJob(controlledJob2);

            //启动job
            Thread th = new Thread(jobControl);
            th.start();

            while(true){
                if(jobControl.allFinished()){
                    System.out.println(jobControl.getSuccessfulJobList());
                    jobControl.stop();
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
