package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalyzeMapper2 extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text dateKey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // 检查字段数是否满足要求
        if (fields.length >= 3) {
            // 处理日期
            String date = fields[0];
            if (!date.equals("mfd_date")){
                dateKey.set(date);
                
                String mfd7 = fields[2];
                double interest = 0.0;
                if (!mfd7.isEmpty() && isNumeric(mfd7)) {
                    interest = Double.parseDouble(mfd7);
                }

                context.write(dateKey, new DoubleWritable(-interest)); 
            }
        }
    }

    // 检查字符串是否为数字
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+(\\.\\d+)?"); // 支持小数
    }
}
