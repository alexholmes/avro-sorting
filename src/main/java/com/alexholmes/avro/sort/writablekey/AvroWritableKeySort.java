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

package com.alexholmes.avro.sort.writablekey;

import com.alexholmes.avro.Weather;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvroWritableKeySort {
    private static class SortMapper
            extends Mapper<AvroKey<Weather>, NullWritable, WeatherSubset, AvroValue<Weather>> {
        @Override
        protected void map(AvroKey<Weather> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            WeatherSubset subset = new WeatherSubset().setStation(key.datum().getStation().toString()).setTime(key.datum().getTime());
            context.write(subset, new AvroValue<Weather>(key.datum()));
        }
    }

    private static class SortReducer
            extends Reducer<WeatherSubset, AvroValue<Weather>, AvroKey<Weather>, NullWritable> {
        @Override
        protected void reduce(WeatherSubset key, Iterable<AvroValue<Weather>> ignore, Context context)
                throws IOException, InterruptedException {
            int i = 1;
            for (AvroValue<Weather> weather : ignore) {
                weather.datum().setCounter(i++);
                context.write(new AvroKey<Weather>(weather.datum()), NullWritable.get());
            }
        }
    }

    public static class WeatherSubsetSortComparator extends WritableComparator {
        public WeatherSubsetSortComparator() {
            super(WeatherSubset.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            WeatherSubset p1 = (WeatherSubset) w1;
            WeatherSubset p2 = (WeatherSubset) w2;
            int cmp = p1.getStation().compareTo(p2.getStation());
            if (cmp != 0) {
                return cmp;
            }
            return new Long(p1.getTime()).compareTo(p2.getTime());
        }
    }

    public static class WeatherSubsetGroupingComparator extends WritableComparator {
        public WeatherSubsetGroupingComparator() {
            super(WeatherSubset.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            WeatherSubset p1 = (WeatherSubset) w1;
            WeatherSubset p2 = (WeatherSubset) w2;
            return p1.getStation().compareTo(p2.getStation());
        }
    }

    public static class WeatherSubset implements WritableComparable<WeatherSubset> {
        private String station;
        private long time;

        @Override
        public void readFields(DataInput in) throws IOException {
            this.station = in.readUTF();
            this.time = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(station);
            out.writeLong(time);
        }

        @Override
        public int compareTo(WeatherSubset other) {
            int compare = this.station.compareTo(other.station);
            if (compare != 0) {
                return compare;
            }
            return new Long(time).compareTo(other.time);
        }

        public WeatherSubset setStation(String station) {
            this.station = station;
            return this;
        }

        public WeatherSubset setTime(long time) {
            this.time = time;
            return this;
        }

        public String getStation() {
            return station;
        }

        public long getTime() {
            return time;
        }
    }

    public static class WeatherPartitioner extends
            Partitioner<WeatherSubset, AvroValue<Weather>> {
        @Override
        public int getPartition(WeatherSubset key, AvroValue<Weather> value, int numPartitions) {
            return Math.abs(key.getStation().hashCode() * 127) % numPartitions;
        }
    }

    public boolean runMapReduce(final Job job, Path inputPath, Path outputPath) throws Exception {
        FileInputFormat.setInputPaths(job, inputPath);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, Weather.SCHEMA$);

        job.setMapperClass(SortMapper.class);
        AvroJob.setMapOutputValueSchema(job, Weather.SCHEMA$);
        job.setMapOutputKeyClass(WeatherSubset.class);

        job.setReducerClass(SortReducer.class);
        AvroJob.setOutputKeySchema(job, Weather.SCHEMA$);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setPartitionerClass(WeatherPartitioner.class);
        job.setGroupingComparatorClass(WeatherSubsetGroupingComparator.class);
        job.setSortComparatorClass(WeatherSubsetSortComparator.class);

        return job.waitForCompletion(true);
    }
}
