package com.alexholmes.avro.sort.avrokey;

import org.apache.avro.Schema;
import org.apache.avro.io.AvroDataHack;
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

    public AvroDataHack.OrderedField of(Schema s, String fieldName, boolean ascending) {
        return new AvroDataHack.OrderedField().setField(s.getField(fieldName)).setAscendingOrder(ascending);
    }

    @Test
    public void testJson() {
        assertEquals(0, Foo.SCHEMA$.getField("foo1").pos());
        assertEquals(1, Foo.SCHEMA$.getField("foo2").pos());
        assertEquals(2, Foo.SCHEMA$.getField("foo3").pos());

        assertEquals(0, Bar.SCHEMA$.getField("bar1").pos());
        assertEquals(1, Bar.SCHEMA$.getField("bar2").pos());

        Map<Schema, List<AvroDataHack.OrderedField>> input = new HashMap<Schema, List<AvroDataHack.OrderedField>>();
        input.put(Foo.SCHEMA$, Arrays.asList(
                of(Foo.SCHEMA$, "foo2", true),
                of(Foo.SCHEMA$, "foo1", true),
                of(Foo.SCHEMA$, "foo3", true)));

        input.put(Bar.SCHEMA$, Arrays.asList(of(Bar.SCHEMA$, "bar2", true)));

        String json = AvroSort.schemaFieldsToJson(input);

        System.out.println("JSON = '" + json + "'");

        Map<Schema, List<AvroDataHack.OrderedField>> output = AvroSort.jsonToSchemaFields(json);

        assertEquals(input, output);
    }
}
