package com.ryan.groupingcomparator;

import com.sun.tools.corba.se.idl.constExpr.Or;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 就输出一个OrderBean, 故此输出位为BullWritable
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        orderBean.setOrderid(fields[0]);
        orderBean.setProductid(fields[1]);
        orderBean.setPrice(Double.parseDouble(fields[2]));
        context.write(orderBean, NullWritable.get());
    }
}
