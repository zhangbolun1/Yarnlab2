package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTreeMapper extends Mapper<Object, Text, NullWritable, Text> {
    private final Text districtCountPair = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        districtCountPair.set(value.toString());
        context.write(NullWritable.get(), districtCountPair);
    }
}