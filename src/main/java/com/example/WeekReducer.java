package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class WeekReducer extends Reducer<Text, IntWritable, Text, Text> {
    private TreeMap<Long, String> flow = new TreeMap<>();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        long inflow = 0;
        long outflow = 0;
        int sum1 = 0;
        int sum2 = 0;

        for (IntWritable val : values) {
            if (val.get() > 0) {
                inflow += val.get(); // 资金流入
                sum1++;
            } else {
                outflow += -val.get(); // 资金流出
                sum2++;
            }
        }
        inflow = inflow/sum1;
        outflow = outflow/sum2;

        // 输出格式：<日期> TAB <资⾦流⼊量>,<资⾦流出量>
        flow.put(inflow, key.toString() + "\t" + inflow + "," + outflow);
    }   
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 在 cleanup 中输出排序后的结果
        for (Map.Entry<Long, String> entry : flow.entrySet()) {
            context.write(new Text(entry.getValue()), new Text(""));
        }
    }
}
