package com.ryan.test1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 目的
 * 按照不同的电影分类分区，每个电影分类分区按照年份排序
 */

public class Test1Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(Test1Driver.class);

        job.setMapperClass(Test1Mapper.class);
        job.setReducerClass(Test1Reducer.class);
        job.setMapOutputKeyClass(FilmBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(TestComparator.class);
        job.setInputFormatClass(Test1TextInputFormat.class);

        job.setOutputKeyClass(FilmBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
