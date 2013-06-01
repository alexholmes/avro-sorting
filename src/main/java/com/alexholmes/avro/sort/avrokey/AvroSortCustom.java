/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.alexholmes.avro.sort.avrokey;

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

public class AvroSortCustom {

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
        protected void reduce(AvroKey<WeatherNoIgnore> key, Iterable<AvroValue<WeatherNoIgnore>> ignore, Context context)
                throws IOException, InterruptedException {
            int i = 1;
            for (AvroValue<WeatherNoIgnore> WeatherNoIgnore : ignore) {
                WeatherNoIgnore.datum().setCounter(i++);
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

        AvroSort.builder()
                .setJob(job)
                .addPartitionField(WeatherNoIgnore.SCHEMA$, "station", true)
                .addSortField(WeatherNoIgnore.SCHEMA$, "station", true)
                .addSortField(WeatherNoIgnore.SCHEMA$, "time", true)
                .addSortField(WeatherNoIgnore.SCHEMA$, "temp", true)
                .addGroupField(WeatherNoIgnore.SCHEMA$, "station", true)
                .addGroupField(WeatherNoIgnore.SCHEMA$, "time", true)
                .configure();

        return job.waitForCompletion(true);
    }
}
