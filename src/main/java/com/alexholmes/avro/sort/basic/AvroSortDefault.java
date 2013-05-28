package com.alexholmes.avro.sort.basic;

import com.alexholmes.avro.WeatherNoIgnore;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AvroSortDefault {

    private static class SortMapper
            extends Mapper<AvroKey<WeatherNoIgnore>, NullWritable, AvroKey<WeatherNoIgnore>, AvroValue<WeatherNoIgnore>> {
        @Override
        protected void map(AvroKey<WeatherNoIgnore> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, new AvroValue<WeatherNoIgnore>(key.datum()));
        }
    }

    private static class SortReducer
            extends Reducer<AvroKey<WeatherNoIgnore>, AvroValue<WeatherNoIgnore>, AvroKey<WeatherNoIgnore>, NullWritable> {
        @Override
        protected void reduce(AvroKey<WeatherNoIgnore> key, Iterable<AvroValue<WeatherNoIgnore>> values, Context context)
                throws IOException, InterruptedException {
            int counter = 1;
            for (AvroValue<WeatherNoIgnore> WeatherNoIgnore : values) {
                WeatherNoIgnore.datum().setCounter(counter++);
                context.write(new AvroKey<WeatherNoIgnore>(WeatherNoIgnore.datum()), NullWritable.get());
            }
        }
    }

    public boolean runMapReduce(final Job job, Path inputPath, Path outputPath) throws Exception {
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, WeatherNoIgnore.SCHEMA$);

        job.setMapperClass(SortMapper.class);
        AvroJob.setMapOutputKeySchema(job, WeatherNoIgnore.SCHEMA$);
        AvroJob.setMapOutputValueSchema(job, WeatherNoIgnore.SCHEMA$);

        job.setReducerClass(SortReducer.class);
        AvroJob.setOutputKeySchema(job, WeatherNoIgnore.SCHEMA$);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }
}
