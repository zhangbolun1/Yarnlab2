//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.opstty.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TreeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    public TreeReducer() {
    }

    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long sum = 0L;

        LongWritable val;
        for(Iterator var6 = values.iterator(); var6.hasNext(); sum += val.get()) {
            val = (LongWritable)var6.next();
        }

        context.write(key, new LongWritable(sum));
    }
}
