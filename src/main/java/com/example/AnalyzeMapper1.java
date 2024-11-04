package com.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalyzeMapper1 extends Mapper<Object, Text, Text, DoubleWritable> {
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
                
                String purchase_bal_amt = fields[6];
                double purchaseAmt = 0.0;
                if (!purchase_bal_amt.isEmpty() && isNumeric(purchase_bal_amt)) {
                    purchaseAmt = Double.parseDouble(purchase_bal_amt);
                }

                context.write(dateKey, new DoubleWritable(purchaseAmt)); 
            }
        }
    }

    // 检查字符串是否为数字
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+");
    }
}
