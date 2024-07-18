package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text kind = new Text();
    private DoubleWritable height = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            // Skip header row
            return;
        }
        String[] fields = value.toString().split(";");
        if (fields.length > 6) {  // Assuming kind is the fourth column and height is the seventh column
            kind.set(fields[3]);
            try {
                height.set(Double.parseDouble(fields[6]));
                context.write(kind, height);
            } catch (NumberFormatException e) {
                // Handle parse error
            }
        }
    }
}