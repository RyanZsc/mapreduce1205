package com.ryan.test1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TestComparator extends WritableComparator {

    FilmBean oa;
    FilmBean ob;

    protected TestComparator() {
        super(FilmBean.class, null, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        oa = (FilmBean) a;
        ob = (FilmBean) b;

        // 只比较类型,若相等，就返回0
        return oa.getCategory().compareTo(ob.getCategory());
    }
}
