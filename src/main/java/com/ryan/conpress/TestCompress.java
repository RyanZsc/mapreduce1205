package com.ryan.conpress;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import javax.print.attribute.standard.Compression;
import java.io.*;

public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        // 压缩
        // compress("D:\\Project\\test\\film1.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        // compress("D:\\Project\\test\\film1.txt", "org.apache.hadoop.io.compress.GzipCodec");
        // compress("D:\\Project\\test\\film1.txt", "org.apache.hadoop.io.compress.DefaultCodec");
        decompress("D:\\Project\\test\\CompressionTest\\film1.txt.deflate");

    }

    private static void compress(String filename, String method) throws IOException, ClassNotFoundException {
        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        Class codecClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024, false);

        // 关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    private static void decompress(String filename) throws IOException, ClassNotFoundException {
        // 压缩方式检查, codec：压缩方式
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null) {
            System.out.println("Cannot find codec for file " + filename);
            return;
        }
        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(filename));
        CompressionInputStream cis = codec.createInputStream(fis);

        // 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(filename + ".decode"));

        // 流的拷贝
        IOUtils.copyBytes(cis, fos, 1024*1024, false);

        // 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }

}
