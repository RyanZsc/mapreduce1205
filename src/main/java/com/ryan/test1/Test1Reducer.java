package com.ryan.test1;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Test1Reducer extends Reducer<FilmBean, NullWritable, FilmBean, NullWritable> {
    private Iterator<NullWritable> iterator;

    @Override
    protected void reduce(FilmBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        iterator = values.iterator();
        while (iterator.hasNext()) {
            NullWritable value = iterator.next();
            context.write(key, value);
        }
    }
}
