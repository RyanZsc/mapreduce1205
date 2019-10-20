package com.ryan.writablecomparable2;

import com.ryan.writablecomparable.FlowBean;
import com.ryan.writablecomparable.SortMapper;
import com.ryan.writablecomparable.SortRecuder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortRecuder.class);

        job.setPartitionerClass(MyPartitioner2.class);
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\Project\\test\\writablecomparableOutput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Project\\test\\writablecomparable2Output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
