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

package com.alexholmes.avro.sort.basic;

import com.alexholmes.avro.WeatherNoIgnore;
import com.alexholmes.avro.sort.AbstractAvroTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroSortDefault extends AbstractAvroTest {

    @Test
    public void testAvroMapOutput() throws Exception {

        Path inputPath = new Path(writeWeatherNoIgnoreInput().getAbsolutePath());
        Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/output");

        Job job = new Job();

        Assert.assertTrue(new AvroSortDefault().runMapReduce(job, inputPath, outputPath));

        super.assertOutputResults(job, outputPath, new WeatherNoIgnore[] {
                WeatherNoIgnore.newBuilder().setStation("IAD").setTime(1).setTemp(1).setCounter(1).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(1).setTemp(1).setCounter(1).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(1).setTemp(2).setCounter(1).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(1).setTemp(3).setCounter(1).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(2).setTemp(1).setCounter(1).build(),
        });
    }
}
