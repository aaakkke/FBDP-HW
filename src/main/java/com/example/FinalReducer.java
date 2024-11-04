package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FinalReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double totalBuy = 0.0;
        double sum = 0;
        for (DoubleWritable val : values) {
            totalBuy += val.get();
            sum ++;
        }
        totalBuy = totalBuy/sum;

        context.write(key, new DoubleWritable(totalBuy));
    }
}
