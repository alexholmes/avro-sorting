package com.alexholmes.avro.sort.basic;

import org.apache.avro.reflect.ReflectData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestOrdinalSort {

    @Test
    public void testJson() {

        Ordinal o1 = Ordinal.newBuilder().setCfield1(1).setBfield2(2).setAfield3(3).build();
        Ordinal o2 = Ordinal.newBuilder().setCfield1(3).setBfield2(2).setAfield3(1).build();

        assertEquals(-1, ReflectData.get().compare(o1, o2, Ordinal.SCHEMA$));
    }
}
