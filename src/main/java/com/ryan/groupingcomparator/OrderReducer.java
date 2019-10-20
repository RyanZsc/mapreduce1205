package com.ryan.groupingcomparator;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 取第一个
        // context.write(key, NullWritable.get());
//        for (NullWritable value : values) {
//            context.write(key, value);
//        }

        // 取全部
//        Iterator<NullWritable> iterator = values.iterator();
//        while (iterator.hasNext()) {
//            NullWritable value = iterator.next();
//            context.write(key, value);
//        }

        // 取前两名
        Iterator<NullWritable> iterator = values.iterator();
        for (int i = 0; i < 2; i++) {
            if (iterator.hasNext()) {
                context.write(key, iterator.next());
            }
        }
    }
}
