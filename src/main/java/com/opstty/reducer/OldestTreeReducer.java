package com.opstty.reducer;

import com.opstty.writable.TreeAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, TreeAgeWritable, Text, IntWritable> {
    private IntWritable oldestAge = new IntWritable(Integer.MAX_VALUE);
    private Text district = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<TreeAgeWritable> values, Context context) throws IOException, InterruptedException {
        for (TreeAgeWritable val : values) {
            if (val.getAge().get() < oldestAge.get()) {
                oldestAge.set(val.getAge().get());
                district.set(val.getDistrict());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(district, oldestAge);
    }
}