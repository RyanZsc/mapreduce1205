package com.ryan.writablecomparable2;

import com.ryan.writablecomparable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner2 extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int i) {
        switch (text.toString().substring(0, 3)) {
            case "135":
                return 0;
            case "158":
                return 1;
            case "182":
                return 2;
            case "137":
                return 3;
            default:
                return 4;
        }
    }
}