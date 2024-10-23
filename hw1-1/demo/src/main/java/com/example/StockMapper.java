package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text stock = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 将行分割为数组，假设股票代码在第4列
        String[] fields = value.toString().split(",");
        if (fields.length > 3) {
            stock.set(fields[3].trim()); // 获取股票代码
            context.write(stock, one);    // 输出股票代码和计数1
        }
    }
}

