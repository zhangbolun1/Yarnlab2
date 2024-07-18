package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    private Text resultDistrict = new Text();
    private IntWritable maxCount = new IntWritable();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        String district = "";
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            String currentDistrict = parts[0];
            int currentCount = Integer.parseInt(parts[1]);
            if (currentCount > max) {
                max = currentCount;
                district = currentDistrict;
            }
        }
        resultDistrict.set(district);
        maxCount.set(max);
        context.write(resultDistrict, maxCount);
    }
}