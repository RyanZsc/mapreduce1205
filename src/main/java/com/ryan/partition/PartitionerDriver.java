package com.ryan.partition;

import com.ryan.flow.FlowBean;
import com.ryan.flow.FlowMapper;
import com.ryan.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 获取Job实例
        Job job = Job.getInstance(new Configuration());

        //2. 设置类路径
        job.setJarByClass(PartitionerDriver.class);

        //3. 设置Mapper和Reducer的路径
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);

        //4. 设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\Project\\test\\paritionInput"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\Project\\test\\paritionOutput"));

        //6. 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
