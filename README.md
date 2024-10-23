# 2024金融大数据课程仓库

## 作业5
任务5我选择利用vscode+java+maven进行实现。
### 任务一
#### Map类
通过搜索发现CSV文件每个单元格是通过逗号作为分隔符，因此分割方式和wordcount一致。利用逗号进行分割，分割后的数据存入string数组中，然后在数组中取出与股票代码对应的内容。在map过程将出现的每个股票代码计数为1。
##### 注意事项
在analyst_ratings.csv中的headline列中也存在逗号，因此获取股票代码不能就简单的选择数组中索引3所对应的值，而是选取数组中的最后一个元素。
#### Reduce类
针对不同的股票代码进行计数，即将相同股票代码的所有“1”组合到一起，然后相加，在此利用.get()函数去获取数值。最后，将股票代码和次数存入 TreeMap 进行排序，然后按照要求输出。
#### 主类
主类主要负责组合所有的流程，让项目按序进行。
#### 运行
通过javac -classpath命令将.java转换成.class；
随后利用jar -cvf将其压缩成jar包；
最后在Hadoop伪分布模式中用./bin/hadoop jar /home/zhangke/KE/demo/StockCounter.jar com.example.StockCount input output命令运行出结果放置在output中，结果网页截图如下：
![image](https://github.com/aaakkke/FBDP-HW/blob/main/task1%E8%BF%90%E8%A1%8C%E7%BB%93%E6%9E%9C%E6%88%AA%E5%9B%BE.png)
### 任务二
#### Map类
首先用HDFS中的API：FileSystem读取停词文件，将读取的所有单词存储在哈希表中。随后还是以逗号作为分隔符，因为headline中也存在逗号故也会被分割，所以和headline的内容我选择用数组中的第二个与第三个一直到倒数第三个元素进行拼接组成，当然中间需要加上空格符。因为要忽略符号和大小写，所以我将不是字母、数字和空格的内容都用空格代替，然后将所有内容都转换成小写。利用StringTokenizer读取headline中的一个个词，在判断这些词是否出现在停词哈希表中，如果是就忽略。再者，考虑到要求输出的是单词，所以将纯数字、单个字母以及数字+字母的组合也忽略。完成上述的判断后，对符合条件的所有单词计数为1。
#### Reduce类
针对不同的单词进行计数，方法和前面一样。排序也是选择将单词及其次数存入 TreeMap 进行排序。
#### 主类
主类和任务一一样主要负责组合所有的流程，让项目按序进行。为了性能提升也许可以加入combiner过程，不过此次数据集大小还好，所有不加也很快。
#### 运行
通过javac -classpath命令将.java转换成.class；
随后利用jar -cvf将其压缩成jar包；
最后在Hadoop伪分布模式中用./bin/hadoop jar /home/zhangke/ke-hw/demo/WordCounter.jar com.example.WordCount input output命令运行出结果放置在output中，结果网页截图如下：




