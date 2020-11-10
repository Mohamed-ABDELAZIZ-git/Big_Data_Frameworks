package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class reducer3 extends Reducer<Text, IntWritable, Text, Iterable<IntWritable>> {

    private final IntWritable Trees = new IntWritable();

    public void reduce(Text t_key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {

        int NbTrees = 0;

        for (IntWritable value : values)
        {
            NbTrees += value.get();
        }

        Trees.set(NbTrees);

        context.write(t_key, (Iterable<IntWritable>) Trees);
    }
}