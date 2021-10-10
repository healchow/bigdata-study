package com.healchow.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 主类，描述并提交 Job 任务
 *
 * @author healchow
 * @since 2021/10/08 22:45
 */
public class JobMain extends Configured implements Tool {

    private static final String HDFS_SERVER_URI = "hdfs://hadoop:9000";

    /**
     * 在 main 方法中提交 Job 任务
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Tool tool = new JobMain();
        int run = ToolRunner.run(configuration, tool, args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        // 可以从运行参数中获取输入和输出路径，省略

        String inputPath = HDFS_SERVER_URI + "/test/wordcount/wordcount.txt";
        String outputPath = HDFS_SERVER_URI + "/test/wordcount/wordcount_out";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCount");
        // 打包到集群上运行时候，必须指定程序的主类
        job.setJarByClass(JobMain.class);

        // 1、读取输入文件解析成key-value
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath));

        // 2、设置 Mapper Class，及 map 阶段完成之后的输出类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 3-6 此例不涉及，暂时省略

        // 7、设置 Reducer Class，及 reduce 阶段完成之后的输出类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 8、设置输出类以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}
