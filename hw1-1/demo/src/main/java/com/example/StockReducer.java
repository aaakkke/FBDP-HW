package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class StockReducer extends Reducer<Text, IntWritable, Text, Text> {
    private TreeMap<Integer, String> sortedStocks = new TreeMap<>(); // 用于存储排序结果

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 将股票代码和次数存入 TreeMap 进行排序
        sortedStocks.put(sum, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 输出格式为 "<次数>：<股票代码>"
        int rank = 1;
        for (Map.Entry<Integer, String> entry : sortedStocks.descendingMap().entrySet()) {
            String output = rank + ":" + entry.getValue() + "," + entry.getKey();
            context.write(new Text(output), null); // 输出结果
            rank++;
        }
    }
}
