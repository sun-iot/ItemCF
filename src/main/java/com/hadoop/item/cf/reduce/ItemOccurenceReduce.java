package com.hadoop.item.cf.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author: Sun
 * @created: 2020/6/12 23:10
 */
public class ItemOccurenceReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     *  * 第二步，计算物品同现矩阵
     *  * 输入是第一步的输出
     *  * 在reduce阶段所做的就是根据key对value进行累加输出。
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
