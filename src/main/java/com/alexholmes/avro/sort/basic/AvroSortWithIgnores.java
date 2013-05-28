package com.alexholmes.avro.sort.basic;

import com.alexholmes.avro.Weather;
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

public class AvroSortWithIgnores {

    private static class SortMapper
            extends Mapper<AvroKey<Weather>, NullWritable, AvroKey<Weather>, AvroValue<Weather>> {
        @Override
        protected void map(AvroKey<Weather> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(key, new AvroValue<Weather>(key.datum()));
        }
    }

    private static class SortReducer
            extends Reducer<AvroKey<Weather>, AvroValue<Weather>, AvroKey<Weather>, NullWritable> {
        @Override
        protected void reduce(AvroKey<Weather> key, Iterable<AvroValue<Weather>> values, Context context)
                throws IOException, InterruptedException {
            int counter = 1;
            for (AvroValue<Weather> Weather : values) {
                Weather.datum().setCounter(counter++);
                context.write(new AvroKey<Weather>(Weather.datum()), NullWritable.get());
            }
        }
    }

    public boolean runMapReduce(final Job job, Path inputPath, Path outputPath) throws Exception {
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, Weather.SCHEMA$);

        job.setMapperClass(SortMapper.class);
        AvroJob.setMapOutputKeySchema(job, Weather.SCHEMA$);
        AvroJob.setMapOutputValueSchema(job, Weather.SCHEMA$);

        job.setReducerClass(SortReducer.class);
        AvroJob.setOutputKeySchema(job, Weather.SCHEMA$);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }
}
