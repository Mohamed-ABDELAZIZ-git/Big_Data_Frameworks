package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapper3 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static int COLUMN_USED = 2;
    private final static String CSV_SEPARATOR = ";";
    private final Text VALUE = new Text();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();

        String[] columnData = valueString.split(CSV_SEPARATOR);

        String speciesColumn = columnData[COLUMN_USED];

        VALUE.set(speciesColumn);

        context.write(VALUE, one);

    }
}