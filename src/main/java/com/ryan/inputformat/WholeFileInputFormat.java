package com.ryan.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 需求：将很多个小文件合并成一个SequenceFile文件（SequenceFile文件是Hadoop用来储存二进制形式的key-value对的文件格式）
 * SequenceFile里面储存着多个文件，储存的形式为文件路径+名称为key，文件内容为value
 * 由于此inputformat已经把工作做完了，就不需要MapReduce了
 */


public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    /**
     * 由于需求是合并，为防止切片判断是否可切中始终为false
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WholeFileRecordReader();
    }
}