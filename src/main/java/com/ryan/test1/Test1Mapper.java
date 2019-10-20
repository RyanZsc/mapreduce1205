package com.ryan.test1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * 无法在这里切分字符，这不是mapper的工作！！！
 * 写一个InputFormat切掉吧
 */
public class Test1Mapper extends Mapper<Text, NullWritable, FilmBean, NullWritable> {

    private FilmBean fb = new FilmBean();
    private String[] fields;

    @Override
    protected void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
        fields = key.toString().split(" ");

        //切掉.0，如将1992.0变成1992
//        String yy = fields[2].replaceAll(".0", "");
//        System.out.println(yy);

        fb.setCategory(fields[3]);
        fb.setFilmname(fields[1]);
        fb.setFilmtime(Integer.parseInt(fields[2]));

        context.write(fb, NullWritable.get());
    }
}
