package com.alexholmes.avro.sort;

import com.alexholmes.avro.AvroFiles;
import com.alexholmes.avro.Weather;
import com.alexholmes.avro.WeatherNoIgnore;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 */
public abstract class AbstractAvroTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    public File writeWeatherInput() throws IOException {
        File input = tmpFolder.newFile("input.txt");
        AvroFiles.createFile(input, Weather.SCHEMA$, Arrays.asList(
                Weather.newBuilder().setStation("SFO").setTime(1).setTemp(3).build(),
                Weather.newBuilder().setStation("IAD").setTime(1).setTemp(1).build(),
                Weather.newBuilder().setStation("SFO").setTime(2).setTemp(1).build(),
                Weather.newBuilder().setStation("SFO").setTime(1).setTemp(2).build(),
                Weather.newBuilder().setStation("SFO").setTime(1).setTemp(1).build()
        ).toArray());
        return input;
    }

    public File writeWeatherNoIgnoreInput() throws IOException {
        File input = tmpFolder.newFile("input.txt");
        AvroFiles.createFile(input, WeatherNoIgnore.SCHEMA$, Arrays.asList(
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(1).setTemp(3).build(),
                WeatherNoIgnore.newBuilder().setStation("IAD").setTime(1).setTemp(1).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(2).setTemp(1).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(1).setTemp(2).build(),
                WeatherNoIgnore.newBuilder().setStation("SFO").setTime(1).setTemp(1).build()
        ).toArray());
        return input;
    }

    public List<Object> writeAvroFilesToStdout(Job job, Path outputPath) throws IOException {
        List<Object> records = new ArrayList<Object>();
        // Check that the results from the MapReduce were as expected.
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
        Assert.assertEquals(1, outputFiles.length);
        DataFileReader<Object> reader = new DataFileReader<Object>(
                new FsInput(outputFiles[0].getPath(), job.getConfiguration()),
                new ReflectDatumReader<Object>());
        for (Object record : reader) {
            records.add(record);
            System.out.println(record);
        }
        reader.close();
        return records;
    }

    public void assertOutputResults(Job job, Path outputPath, Object[] expectedOutputs) throws IOException {
        List<Object> records = new ArrayList<Object>();
        // Check that the results from the MapReduce were as expected.
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
        Assert.assertEquals(1, outputFiles.length);
        DataFileReader<Object> reader = new DataFileReader<Object>(
                new FsInput(outputFiles[0].getPath(), job.getConfiguration()),
                new ReflectDatumReader<Object>());
        for (Object record : reader) {
            records.add(record);
            System.out.println(record);
        }
        reader.close();

        assertArrayEquals(expectedOutputs, records.toArray());
    }

}
