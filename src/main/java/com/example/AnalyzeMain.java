package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: AnalyzeJob <input path> <output path for job1> <output path for job2>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "First Analyze Job");

        job1.setJarByClass(AnalyzeMain.class);

        // 设置 Mapper 和 Reducer 类
        MultipleInputs.addInputPath(job1, new Path(args[0] + "/user_balance_table.csv"), org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, AnalyzeMapper1.class);
        MultipleInputs.addInputPath(job1, new Path(args[0] + "/mfd_day_share_interest.csv"), org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, AnalyzeMapper2.class);

        job1.setReducerClass(AnalyzeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        Path outputPathJob1 = new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, outputPathJob1);

        // 检查并删除作业1输出路径（如果存在）
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPathJob1)) {
            fs.delete(outputPathJob1, true); // true表示递归删除
        }

        // 提交第一个作业并等待完成
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // 设置第二个作业
        Job job2 = Job.getInstance(conf, "Final Analyze Job");
        job2.setJarByClass(AnalyzeMain.class);
        job2.setMapperClass(Job2Mapper.class); // 使用一个空的 Mapper
        job2.setReducerClass(FinalReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);

        Path outputPathJob2 = new Path(args[2]);
        FileOutputFormat.setOutputPath(job2, outputPathJob2);

        // 设置输入路径为第一个作业的输出
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job2, outputPathJob1);

        // 检查并删除作业2输出路径（如果存在）
        if (fs.exists(outputPathJob2)) {
            fs.delete(outputPathJob2, true); // true表示递归删除
        }

        // 提交第二个作业并等待完成
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
