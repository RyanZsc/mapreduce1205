package com.ryan.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// LongWritable: 偏移量  Text: 内容  Text, IntWritable: 希望输出(String, int)形式
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 由于大数据会大量调用map方法，尽量宏定义！
    private Text word = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拿到这一行数据
        String line = value.toString();

        // 按照空格切分
        String[] words = line.split(" ");

        // 遍历数组，把单词变成(word, 1)的形式交给框架
        for (String word : words) {
            this.word.set(word);
            context.write(this.word, this.one);
        }
    }
}
