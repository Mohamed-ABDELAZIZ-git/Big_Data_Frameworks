package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class mapper1 extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final static int COLUMN_USED = 1;
    private final static String CSV_SEPARATOR = ";";
    private final Text VALUE = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String valueString = value.toString();

        String[] columnData = valueString.split(CSV_SEPARATOR);

        String districtColumn = columnData[COLUMN_USED];

        VALUE.set(districtColumn);

        context.write(VALUE, NullWritable.get());

    }
}