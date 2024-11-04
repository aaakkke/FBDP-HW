package com.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 假设 job1 输出的格式是 "<categoryKey>\t<buy>"
        String line = value.toString();
        String[] fields = line.split("\t"); // 根据实际分隔符调整

        if (fields.length == 2) {
            // 将第一个字段作为 key，第二个字段转换为 Double 作为 value
            Text categoryKey = new Text(fields[0]);
            DoubleWritable buyAmount = new DoubleWritable(Double.parseDouble(fields[1]));
            context.write(categoryKey, buyAmount);
        }
    }
}
