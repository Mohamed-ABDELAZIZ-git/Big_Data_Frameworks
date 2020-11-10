package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapper2 extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final static int COLUMN_USED = 2;
    private final static String CSV_SEPARATOR = ";";
    private final Text VALUE = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();

        String[] columnData = valueString.split(CSV_SEPARATOR);

        String speciesColumn = columnData[COLUMN_USED];

        VALUE.set(speciesColumn);

        context.write(VALUE, NullWritable.get());

    }
}