package com.ryan.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    /**
     * 自定义方法：初始化
     * @param taskAttemptContext
     */
    public void initialize(TaskAttemptContext taskAttemptContext) throws IOException {
        // 为了将这两个文件夹写进output，我们需要获取输出路径
        String outdir = taskAttemptContext.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
        // 获取文件系统
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        atguigu = fileSystem.create(new Path(outdir + "/atguigu.log"));
        other = fileSystem.create(new Path(outdir + "/other.log"));

//        atguigu = new FileOutputStream("D:\\Project\\test\\outputformatoutput\\atguigu.log");
//        other = new FileOutputStream("D:\\Project\\test\\outputformatoutput\\other.log");
    }

    /**
     * 将KV值写出，每对KV调用一次
     * @param longWritable
     * @param text
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {
        String out = text.toString() + "\n";
        if (out.contains("atguigu")) {
            atguigu.write(out.getBytes());
        }else {
            other.write(out.getBytes());
        }

    }

    /**
     * 关闭资源
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(other);
    }
}
