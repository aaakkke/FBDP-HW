package com.example;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, Text>{
    private TreeMap<Integer, String> wordcount = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }
        wordcount.put(sum, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        int rank = 1;
        for(Map.Entry<Integer, String> temp: wordcount.descendingMap().entrySet()){
            String output = rank + ":" + temp.getValue() + "," + temp.getKey();
            context.write(new Text(output), null);
            if(rank==100){
                break;
            }
            rank++;
        }
    }
}
