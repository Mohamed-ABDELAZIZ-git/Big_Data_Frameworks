package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

    public class reducer1 extends Reducer<Text, IntWritable, Text, Iterable<IntWritable>> {

        public void reduce(Text t_key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {

            context.write(t_key, null);
        }
    }