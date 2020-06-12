package com.hadoop.item.cf.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @author: Sun
 * @created: 2020/6/12 23:16
 */
public class ScoreMatricMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
        String line = value.toString();
        String[] str = line.split(",");
        if (str.length == 3) {
            Text k = new Text(str[0]);
            Text v = new Text(str[1]+":"+str[2]);
            context.write(k, v);
        }
    }
}
