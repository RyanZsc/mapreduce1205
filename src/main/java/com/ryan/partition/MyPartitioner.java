package com.ryan.partition;

import com.ryan.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();

        switch (phone.substring(0, 3)){
            case "158":
                return 0;
            case "137":
                return 1;
            case "182":
                return 2;
            case "127":
                return 3;
            default:
                return 4;
        }
    }
}
