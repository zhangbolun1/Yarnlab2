//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable one = new LongWritable(1L);
    private Text district = new Text();

    public TreeMapper() {
    }

    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        if (key.get() != 0L || !value.toString().contains("GEOPOINT")) {
            String[] fields = value.toString().split(";");
            if (fields.length > 1) {
                this.district.set(fields[1]);
                context.write(this.district, one);
            }

        }
    }
}
