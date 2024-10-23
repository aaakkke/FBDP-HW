package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text stockCode = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(","); // 分割CSV行
        if (fields.length > 3) {
            stockCode.set(fields[fields.length-1]); // 获取股票代码列
            context.write(stockCode, one); // 输出 (股票代码, 1)
        }
    }
}

