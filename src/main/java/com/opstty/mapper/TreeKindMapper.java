package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeKindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text kind = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            // Skip header row
            return;
        }
        String[] fields = value.toString().split(";");
        if (fields.length > 3) {  // Assuming kind is the fourth column
            kind.set(fields[3]);
            context.write(kind, one);
        }
    }
}