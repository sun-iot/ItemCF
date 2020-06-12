package com.hadoop.item.cf.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Sun
 * @created: 2020/6/12 23:16
 */
public class ScoreMatricReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
        String str = new String();
        for (Text v : value) {
            str += ",";
            str += v.toString();
        }
        v.set(str.replaceFirst(",", ""));
        context.write(key, v);
    }
}
