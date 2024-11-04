package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnalyzeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double buy = 0.0;
        double interest = 0.0;

        // 分类后的 key
        Text categoryKey = new Text();

        for (DoubleWritable val : values) {
            if (val.get() >= 0) {
                buy += val.get(); 
            } else {
                interest = -val.get();
                if (interest >= 0 && interest < 5) {
                    categoryKey.set("1");
                } else if (interest >= 5 && interest < 6) {
                    categoryKey.set("2");
                } else if (interest > 6) {
                    categoryKey.set("3");
                }
            }
        }

        // 输出分类后的 key 和购买量
        context.write(new Text(categoryKey), new DoubleWritable(buy));
    }
}
