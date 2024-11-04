package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserBalanceMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text dateKey = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // 检查字段数是否满足要求
        if (fields.length >= 10) {
            // 处理日期
            String date = fields[1];
            if (!date.equals("report_date")){
                dateKey.set(date);
                
                // 处理资金流入量
                String purchaseStr = fields[4];
                int purchaseAmt = 0;
                if (!purchaseStr.isEmpty() && isNumeric(purchaseStr)) {
                    purchaseAmt = Integer.parseInt(purchaseStr);
                }

                // 处理资金流出量
                String redeemStr = fields[8];
                int redeemAmt = 0;
                if (!redeemStr.isEmpty() && isNumeric(redeemStr)) {
                    redeemAmt = Integer.parseInt(redeemStr);
                }

                // 输出流入和流出
                context.write(dateKey, new IntWritable(purchaseAmt)); // 流入
                context.write(dateKey, new IntWritable(-redeemAmt)); // 流出
            }
        }
    }

    // 检查字符串是否为数字
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+");
    }
}
