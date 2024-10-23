# 2024金融大数据课程仓库

## 作业5
任务5我选择利用vscode+java+maven进行实现。
### 任务一
#### Map类
通过搜索发现CSV文件每个单元格是通过逗号作为分隔符，因此分割方式和wordcount一致。利用逗号进行分割，分割后的数据存入string数组中，然后在数组中取出与股票代码对应的内容。在map过程将出现的每个股票代码计数为1。
#### Reduce类
针对不同的股票代码进行计数，即将相同股票代码的所有“1”组合到一起，然后相加，在此利用.get()函数去获取数值。
#### 主类
主类主要负责组合所有的流程，让项目按序进行。
#### 运行
通过javac -classpath命令将.java转换成.class；
随后利用jar -cvf将其压缩成jar包；
最后在Hadoop伪分布模式中用./bin/hadoop jar /home/zhangke/KE/demo/StockCounter.jar com.example.StockCount input output命令运行出结果放置在output中，结果网页截图如下：
![image](https://github.com/aaakkke/FBDP-HW/blob/main/task1%E8%BF%90%E8%A1%8C%E7%BB%93%E6%9E%9C%E6%88%AA%E5%9B%BE.png)
### 任务二
#### Map类



