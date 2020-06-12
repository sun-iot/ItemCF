package com.hadoop.item.cf.reduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Sun
 * @created: 2020/6/12 23:15
 */
public class RecommendReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    Text k = new Text();
    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,
                          Context context) throws IOException, InterruptedException {
        double totalScore = 0.0;
        for(DoubleWritable d : values)
        {
            totalScore += d.get();
        }
        String str[] = key.toString().split(":");
        k.set(str[0]);
        v.set(str[1] + ":" + totalScore);
        context.write(k, v);

    }
}
