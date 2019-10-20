package com.ryan.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String orderid;
    private String productid;
    private double price;

    @Override
    public String toString() {
        return orderid + "\t" + productid + "\t" + price;
    }

    public String getOrderid() {
        return orderid;
    }

    public void setOrderid(String orderid) {
        this.orderid = orderid;
    }

    public String getProductid() {
        return productid;
    }

    public void setProductid(String productid) {
        this.productid = productid;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    /**
     * 比较规则
     * @param o
     * @return 先比较订单ID，如果相等再比较价格，不等则按照订单来排序
     */
    @Override
    public int compareTo(OrderBean o) {
        int compare = this.orderid.compareTo(o.orderid);

        if (compare == 0) {
            return Double.compare(o.price, this.price);
        }else {
            return compare;
        }
    }

    /**
     * 序列化方法
     * @param dataOutput 框架给我们提供的数据出口
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderid);
        dataOutput.writeUTF(productid);
        dataOutput.writeDouble(price);
    }

    /**
     * 反序列化方法
     * @param dataInput 框架提供的数据来源
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderid = dataInput.readUTF();
        this.productid = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }
}
