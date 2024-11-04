package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeekMain {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WeekMain.class);
        job.setMapperClass(WeekMapper.class);
        job.setReducerClass(WeekReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(false)? 0 : 1);
    } 
      
    
}
