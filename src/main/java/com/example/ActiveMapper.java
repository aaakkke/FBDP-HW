package com.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActiveMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text userid = new Text();

    @Override
    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        String[] temps = value.toString().split(",");
        if(!temps[5].isEmpty()&& isNumeric(temps[5])){
            if(Integer.parseInt(temps[5])>0){
                userid.set(temps[0]);
                context.write(userid, one);
                return;
            }
        }
        else if(!temps[8].isEmpty()&& isNumeric(temps[8])){
            if(Integer.parseInt(temps[8])>0){
                userid.set(temps[0]);
                context.write(userid, one);
                return;
            }
        }


    }
    // 检查字符串是否为数字
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+");
    }
}
