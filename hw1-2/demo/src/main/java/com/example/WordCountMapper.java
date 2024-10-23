package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jline.utils.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
//import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;


public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private HashSet<String> stopWords = new HashSet<>();
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        // 使用HDFS API读取stop-word-list.txt
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path("input/stop-word-list.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = br.readLine()) != null) {
            stopWords.add(line.trim().toLowerCase());
        }
        br.close();
    }


    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String headline = new String();
        String[] fields = value.toString().split(","); // 分割CSV行
        if (fields.length > 3) {
            for (int k = 1; k < fields.length - 2; k++) {
                headline += fields[k] + " ";
            }
        }
        // Remove punctuation and convert to lowercase
        headline = headline.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();
        StringTokenizer itr = new StringTokenizer(headline);
        while (itr.hasMoreTokens()) {
            String token = itr.nextToken();
            // Check if token is purely numeric or contains both letters and digits
            if (!stopWords.contains(token) && !(token.length()==1 || token.matches("^[0-9]+$") || token.matches(".*[0-9].*"))) {
                word.set(token);
                context.write(word, one);
            }
        }
    }
}
