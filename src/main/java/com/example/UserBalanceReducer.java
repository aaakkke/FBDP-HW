package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserBalanceReducer extends Reducer<Text, IntWritable, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int inflow = 0;
        int outflow = 0;

        for (IntWritable val : values) {
            if (val.get() > 0) {
                inflow += val.get(); // 资金流入
            } else {
                outflow += -val.get(); // 资金流出
            }
        }

        // 输出格式：<日期> TAB <资⾦流⼊量>,<资⾦流出量>
        context.write(key, new Text(inflow + "," + outflow));
    }
}
