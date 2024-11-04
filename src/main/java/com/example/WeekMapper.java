package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

public class WeekMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    private final static String[] weekdays = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t"); // 使用制表符分隔

        // 检查字段数是否满足要求
        if (fields.length == 2) { // 修改为2个字段
            // 处理日期
            Date date = null;
            try {
                date = sdf.parse(fields[0]); // fields[0] 是日期
            } catch (ParseException e) {
                System.err.println("Parse exception for date: " + fields[0]);
                return; // 跳过此记录
            }

            // 处理金额
            String[] amounts = fields[1].split(","); // 分割金额字符串
            int purchaseAmt = isNumeric(amounts[0]) ? Integer.parseInt(amounts[0]) : 0;
            int redeemAmt = (amounts.length > 1 && isNumeric(amounts[1])) ? Integer.parseInt(amounts[1]) : 0;

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK); // 返回1-7（1 = 星期日）

            // 输出流入和流出
            context.write(new Text(weekdays[dayOfWeek - 1]), new IntWritable(purchaseAmt)); // 流入
            context.write(new Text(weekdays[dayOfWeek - 1]), new IntWritable(-redeemAmt)); // 流出
        }
    }

    // 检查字符串是否为数字
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+");
    }
}
