package com.example;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActiveReducer extends Reducer<Text, IntWritable, Text, Text>{
    private TreeMap<Integer,String> sortedusers = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }
        sortedusers.put(sum, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(Map.Entry<Integer, String> entry: sortedusers.descendingMap().entrySet()){
            String output = entry.getValue() + "\t" + entry.getKey();
            context.write(new Text(output), null);
        }
    }

}
