package com.healchow.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义 Mapper 类，继承 org.apache.hadoop.mapreduce.Mapper，实现 map 功能
 *
 * @author healchow
 * @since 2021/10/08 22:45
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable ONE_WORD = new LongWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 每一行字符串的格式：hello,hadoop
        String line = value.toString();
        String[] words = line.split(",");

        // 转换成 <hello, 1>, <hadoop, 1>
        for (String word : words) {
            context.write(new Text(word), ONE_WORD);
        }
    }

}