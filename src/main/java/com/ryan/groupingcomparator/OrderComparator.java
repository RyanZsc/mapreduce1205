package com.ryan.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator {

    protected OrderComparator() {
        super(OrderBean.class, null, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        // 只比较id，若相等，就返回0
        return oa.getOrderid().compareTo(ob.getOrderid());
    }
}
