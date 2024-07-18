package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text species = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 && value.toString().contains("GEOPOINT")) {
            // Skip header row
            return;
        }
        String[] fields = value.toString().split(";");
        if (fields.length > 3) {  // Assuming species is the fourth column
            species.set(fields[3]);
            context.write(species, new Text(""));
        }
    }
}