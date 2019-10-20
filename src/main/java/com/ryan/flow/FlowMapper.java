package com.ryan.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //框架已经帮你把数据一行一行的传进map了
        String[] fields = value.toString().split("\t");
        phone.set(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        flow.set(upFlow, downFlow);

        // 处理好的数据写给框架
        context.write(phone, flow);
    }
}
