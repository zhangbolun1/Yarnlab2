package com.opstty.mapper;

import com.opstty.writable.TreeAgeWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, IntWritable, TreeAgeWritable> {
    private IntWritable age = new IntWritable();
    private TreeAgeWritable treeAgeWritable = new TreeAgeWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            // Skip header row
            return;
        }
        String[] fields = value.toString().split(";");
        if (fields.length > 5) {  // Assuming district is the second column and age is the sixth column
            try {
                age.set(Integer.parseInt(fields[5]));
                treeAgeWritable.setAge(age);
                treeAgeWritable.setDistrict(new Text(fields[1]));
                context.write(age, treeAgeWritable);
            } catch (NumberFormatException e) {
                // Handle parse error
            }
        }
    }
}