package com.healchow.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义 Reducer 类，继承 org.apache.hadoop.mapreduce.Reducer，实现 reduce 功能
 *
 * @author healchow
 * @since 2021/10/08 23:12
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 这里的 key 是单词，values 是单词出现的次数
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 把 <hadoop, 1>, <hadoop, 2> 转换成 <hadoop, 3>
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }

}
