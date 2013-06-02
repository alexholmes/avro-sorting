package com.alexholmes.avro.sort.avrokey;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.io.AvroDataHack;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 *
 */
public class TestAvroSort {

    public static AvroDataHack.OrderedField of(Schema s, String fieldName, boolean ascending) {
        return new AvroDataHack.OrderedField().setField(s.getField(fieldName)).setAscendingOrder(ascending);
    }

    static Map<Schema, List<AvroDataHack.OrderedField>> generateMap() {
        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo2", true),
                of(Foo.SCHEMA$, "foo1", true),
                of(Foo.SCHEMA$, "foo3", true)));

        input.put(Bar.SCHEMA$, Arrays.asList(of(Bar.SCHEMA$, "bar2", true)));
        return input;
    }

    static String generateJsonString() {
        return AvroSort.schemaFieldsToJson(generateMap());
    }

    @Test
    public void testJson() {
        assertEquals(0, Foo.SCHEMA$.getField("foo1").pos());
        assertEquals(1, Foo.SCHEMA$.getField("foo2").pos());
        assertEquals(2, Foo.SCHEMA$.getField("foo3").pos());

        assertEquals(0, Bar.SCHEMA$.getField("bar1").pos());
        assertEquals(1, Bar.SCHEMA$.getField("bar2").pos());

        Map<Schema, List<AvroDataHack.OrderedField>> input = generateMap();
        String json = AvroSort.schemaFieldsToJson(input);

        System.out.println("JSON = '" + json + "'");

        Map<Schema, List<AvroDataHack.OrderedField>> output = AvroSort.jsonToSchemaFields(json);

        assertEquals(input, output);
    }

    @Test
    public void testSingleFieldPartitioner() {
        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo1", true)));

        assertPartitionsEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertPartitionsEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(2).setBar2(3).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());
    }

    @Test
    public void testTwoFieldPartitioner() {
        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo1", true),
                of(Foo.SCHEMA$, "foo2", true)));

        assertPartitionsEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertPartitionsEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(2).setBar2(3).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());
    }

    @Test
    public void testChildRecordFieldPartitioner() {
        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo2", true),
                of(Foo.SCHEMA$, "foo3", true)));
        input.put(Bar.SCHEMA$, Arrays.asList(
                of(Bar.SCHEMA$, "bar2", true)));

        assertPartitionsEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(3).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertPartitionsNotEqual(input,
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(3).build()).build());
    }

    public static void assertPartitionsEqual(Map<Schema, List<AvroDataHack.OrderedField>> input, Foo x, Foo y) {
        Configuration c = new Configuration();
        c.set(AvroSort.PARTITIONING_FIELD_POSITIONS, AvroSort.schemaFieldsToJson(input));

        AvroSort.AvroSecondarySortPartitioner part = new AvroSort.AvroSecondarySortPartitioner();
        part.setConf(c);

        int partitionX = part.getPartition(new AvroKey<SpecificRecordBase>(x), null, 10);
        int partitionY = part.getPartition(new AvroKey<SpecificRecordBase>(y), null, 10);

        assertEquals("x=" + x + " y=" + y, partitionX, partitionY);
    }

    public static void assertPartitionsNotEqual(Map<Schema, List<AvroDataHack.OrderedField>> input, Foo x, Foo y) {
        Configuration c = new Configuration();
        c.set(AvroSort.PARTITIONING_FIELD_POSITIONS, AvroSort.schemaFieldsToJson(input));

        AvroSort.AvroSecondarySortPartitioner part = new AvroSort.AvroSecondarySortPartitioner();
        part.setConf(c);

        int partitionX = part.getPartition(new AvroKey<SpecificRecordBase>(x), null, 10);
        int partitionY = part.getPartition(new AvroKey<SpecificRecordBase>(y), null, 10);

        assertNotSame("x=" + x + " y=" + y, partitionX, partitionY);
    }

    @Test
    public void testSingleFieldComparator() {
        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo1", true)));

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(2).setBar2(3).build()).build());

        assertComparatorLhsLess(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsMore(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(4).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        // descending sort
        input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo1", false)));

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsMore(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsLess(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(4).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());
    }

    @Test
    public void testTwoFieldComparator() {
        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo2", true),
                of(Foo.SCHEMA$, "foo1", true)));

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(2).setBar2(3).build()).build());

        assertComparatorLhsLess(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsLess(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsLess(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(6).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        // descending sort
        input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo2", false),
                of(Foo.SCHEMA$, "foo1", false)));

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorEquals(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(2).setBar2(3).build()).build());

        assertComparatorLhsMore(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(6).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsMore(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(5).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());

        assertComparatorLhsMore(Foo.SCHEMA$, input,
                Foo.newBuilder().setFoo1(6).setFoo2("bar").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build(),
                Foo.newBuilder().setFoo1(5).setFoo2("foo").setFoo3(Bar.newBuilder().setBar1(1).setBar2(2).build()).build());
    }

    public static void assertComparatorEquals(Schema schema, Map<Schema, List<AvroDataHack.OrderedField>> input, Foo x, Foo y) {
        int result = getComparatorResult(schema, input, x, y);
        assertEquals("x=" + x + " y=" + y, 0, result);
    }

    public static void assertComparatorLhsLess(Schema schema, Map<Schema, List<AvroDataHack.OrderedField>> input, Foo x, Foo y) {
        int result = getComparatorResult(schema, input, x, y);
        assertTrue("x=" + x + " y=" + y, result < 0);
    }

    public static void assertComparatorLhsMore(Schema schema, Map<Schema, List<AvroDataHack.OrderedField>> input, Foo x, Foo y) {
        int result = getComparatorResult(schema, input, x, y);
        assertTrue("x=" + x + " y=" + y, result > 0);
    }

    public static int getComparatorResult(Schema schema, Map<Schema, List<AvroDataHack.OrderedField>> input, Foo x, Foo y) {
        Configuration c = new Configuration();
        c.set(AvroSort.SORTING_FIELD_POSITIONS, AvroSort.schemaFieldsToJson(input));
        AvroSerialization.setKeyWriterSchema(c, schema);
        AvroSerialization.setKeyReaderSchema(c, schema);

        AvroSort.AvroSortingComparator comp = new AvroSort.AvroSortingComparator();
        comp.setConf(c);

        return comp.compare(new AvroKey<SpecificRecordBase>(x), new AvroKey<SpecificRecordBase>(y));
    }
}
