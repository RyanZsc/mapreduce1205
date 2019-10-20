package com.ryan.test1;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;

/**
 * 目标，将格式统一转化为      filmID + " " + filmName + " " + filmYear + " " + category
 */
public class Test1RecordReader extends RecordReader<Text, NullWritable> {

    private Text key = new Text();
    private boolean notread = true;
    private BufferedReader br;
    private InputStreamReader isr;
    private FSDataInputStream fileIn;
    private FileSplit split;
    private String line = null;
    private String[] lineSplit = null;
    private String[] year = null;
    private boolean isnum = false;
    private Pattern pattern = Pattern.compile("[0-9]*");

    /**
     * 初始化，尝试打开这个文件，以便在后续方法操作里面的元素，只读取一次的
     * @param inputSplit
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        // 转切片类型
        split = (FileSplit) inputSplit;
        // 通过切片获取路径
        Path path = split.getPath();
        // 通过通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(taskAttemptContext.getConfiguration());
        // 开流, 为了读取一行，要包装成BufferedReader
        fileIn = fileSystem.open(path);
        isr = new InputStreamReader(fileIn);
        br = new BufferedReader(isr);

    }

    /**
     * 尝试读写下一个kv值
     * @return
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notread) {
            // 设置key，key为FilmBean
            // 读取一行，并处理着一行数据
            if ((line = br.readLine()) != null) {
                lineSplit = line.split("\\s+");
                // 按照判断：key分割后，只要长度大于4的、第三位一定是数字的、第四位一定不是数字
                if (lineSplit.length >= 4 && isNum(lineSplit[2]) && !isNum(lineSplit[3])) {
                    // 将年份的小数点杀掉
                    year = lineSplit[2].split("\\.");

                    key.set(lineSplit[0] + " " + lineSplit[1] + " " + year[0] + " " + lineSplit[3]);
                    isnum = false;
                }else {
                    // 不满足条件的行数据，该怎么处理呢？？？
                }
            }else {
                // 读到最后一行了
                notread = false;
            }
            // 不用设置value，下面getcurrentvalue方法写了
            return true;
        }else {
            return false;
        }

    }

    // 判断进来的字符串是不是数字
    private boolean isNum(String s) {
        if(s.indexOf(".")>0){//判断是否有小数点
            if(s.indexOf(".")==s.lastIndexOf(".") && s.split("\\.").length==2){ //判断是否只有一个小数点
                isnum = pattern.matcher(s.replace(".","")).matches();
            }else {
                isnum = false;
            }
        }else {
            isnum = pattern.matcher(s).matches();
        }
        return isnum;
    }

    /**
     * 尝试读取当前Key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 尝试读当前value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 读取当前进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notread ? 0 : 1;
    }

    // 关闭资源
    @Override
    public void close() throws IOException {

    }

    // 操作一行
    public Text operationLine() {
        return null;
    }
}
