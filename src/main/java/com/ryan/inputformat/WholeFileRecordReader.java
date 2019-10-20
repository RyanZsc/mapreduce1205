package com.ryan.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RR，处理一个文件；把这个文件直接读成一个KV值
 */

public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notread = true;

    private Text key = new Text();
    private BytesWritable value = new BytesWritable();
    private FSDataInputStream inputStream;
    private FileSplit fs;

    /**
     * 初始化方法，框架会在开始的时候调用一次
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 转换切片类型到文件切片
        fs = (FileSplit)inputSplit;
        // 通过切片获取路径
        Path path = fs.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());

        // 开流
        inputStream = fileSystem.open(path);
    }

    /**
     * 尝试读取下一组KV值，
     * @return 如果读到了，返回true；没读到，返回false
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(notread){
            // 具体读文件的过程
            // 读key
            key.set(fs.getPath().toString());

            // 读value
            byte[] buf = new byte[(int) fs.getLength()];
            inputStream.read(buf);
            value.set(buf, 0, buf.length);

            notread = false;
            return true;
        }else {
            return false;
        }
    }

    /**
     * 获取当前读到的key
     * @return  当前的key
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前读到的value
     * @return 当前value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * 当前数据读取的进度，由于文件是一次读完，不存在读一半的现象，故只有0和1
     * @return  当前进度
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notread ? 0 : 1;
    }

    /**
     * 关闭资源的
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeStream(inputStream);
    }
}