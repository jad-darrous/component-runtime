/**
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.sdk.component.runtime.di.record;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import routines.system.Dynamic;
import routines.system.DynamicMetadata;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Date;

import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

class DiRowStructVisitorTest extends VisitorsTest {

    private void createMetadata(final Dynamic dynamic, final String name, final String type, final Object value) {
        final DynamicMetadata meta = new DynamicMetadata();
        meta.setName(name);
        meta.setType(type);
        dynamic.metadatas.add(meta);
        dynamic.addColumnValue(value);
    }

    @Test
    void visit() {
        final RowStruct rowStruct = new RowStruct();
        rowStruct.id = ":testing:";
        rowStruct.name = NAME;
        rowStruct.shortP = SHORT;
        rowStruct.shortC = SHORT;
        rowStruct.intP = INT;
        rowStruct.intC = INT;
        rowStruct.longP = LONG;
        rowStruct.longC = LONG;
        rowStruct.floatP = FLOAT;
        rowStruct.floatC = FLOAT;
        rowStruct.doubleP = DOUBLE;
        rowStruct.doubleC = DOUBLE;
        rowStruct.bytes0 = BYTES0;
        rowStruct.date0 = DATE;
        rowStruct.date2 = Date.from(ZONED_DATE_TIME.toInstant());
        rowStruct.bigDecimal0 = BIGDEC;
        rowStruct.bool1 = Boolean.TRUE;
        rowStruct.array0 = INTEGERS;
        rowStruct.object0 = new Rcd();
        rowStruct.hAshcOdEdIrtY = Boolean.TRUE;
        rowStruct.h = NAME;
        // dynamic
        final Dynamic dynamic = new Dynamic();
        createMetadata(dynamic, "dynString", "id_String", "stringy");
        createMetadata(dynamic, "dynInteger", "id_Integer", INT);
        createMetadata(dynamic, "dynDouble", "id_Double", DOUBLE);
        createMetadata(dynamic, "dynBytes", "id_byte[]", BYTES0);
        createMetadata(dynamic, "dynBytesArray", "id_byte[]", BYTES0);
        createMetadata(dynamic, "dynBytesBuffer", "id_byte[]", ByteBuffer.allocate(100).wrap(BYTES0));
        createMetadata(dynamic, "dynBytesWString", "id_byte[]", String.valueOf(BYTES0));
        createMetadata(dynamic, "dynBigDecimal", "id_BigDecimal", BIGDEC);
        createMetadata(dynamic, "dynObject", "id_Object", new Rcd());
        createMetadata(dynamic, "STRINGS", "id_List", STRINGS);
        createMetadata(dynamic, "LONGS", "id_List", LONGS);
        createMetadata(dynamic, "FLOATS", "id_List", FLOATS);
        createMetadata(dynamic, "DOUBLES", "id_List", DOUBLES);
        createMetadata(dynamic, "BOOLEANS", "id_List", BOOLEANS);
        createMetadata(dynamic, "BYTES", "id_List", BYTES);
        createMetadata(dynamic, "DATES", "id_List", DATES);
        createMetadata(dynamic, "RECORDS", "id_List", RECORDS);
        rowStruct.dynamic = dynamic;
        //
        final DiRowStructVisitor visitor = new DiRowStructVisitor();
        final Record record = visitor.get(rowStruct, factory);
        final Schema schema = record.getSchema();
        // should have 3 excluded fields
        assertEquals(42, schema.getEntries().size());
        // asserts Record
        assertEquals(":testing:", record.getString("id"));
        assertEquals(NAME, record.getString("name"));
        assertEquals(SHORT, record.getInt("shortP"));
        assertEquals(SHORT, record.getInt("shortC"));
        assertEquals(INT, record.getInt("intP"));
        assertEquals(INT, record.getInt("intC"));
        assertEquals(LONG, record.getLong("longP"));
        assertEquals(LONG, record.getLong("longC"));
        assertEquals(FLOAT, record.getFloat("floatP"));
        assertEquals(FLOAT, record.getFloat("floatC"));
        assertEquals(DOUBLE, record.getDouble("doubleP"));
        assertEquals(DOUBLE, record.getDouble("doubleC"));
        assertEquals(DATE.toInstant(), record.getDateTime("date0").toInstant());
        assertNull(record.getDateTime("date1"));
        assertEquals(ZONED_DATE_TIME, record.getDateTime("date2"));
        assertEquals(1946, record.getDateTime("date2").getYear());
        assertEquals(BIGDEC.doubleValue(), new BigDecimal(record.getString("bigDecimal0")).doubleValue());
        assertEquals(BIGDEC.toString(), record.getString("bigDecimal0"));
        assertFalse(record.getBoolean("bool0"));
        assertTrue(record.getBoolean("bool1"));
        assertArrayEquals(BYTES0, record.getBytes("bytes0"));
        assertArrayEquals(BYTES0, record.getBytes("dynBytes"));
        assertArrayEquals(BYTES0, record.getBytes("dynBytesArray"));
        assertArrayEquals(BYTES0, record.getBytes("dynBytesBuffer"));
        assertArrayEquals(String.valueOf(BYTES0).getBytes(), record.getBytes("dynBytesWString"));
        assertEquals(BIGDEC.toString(), record.getString("dynBigDecimal"));
        assertEquals(BIGDEC, new BigDecimal(record.getString("dynBigDecimal")));
        assertEquals(RECORD.toString(), record.getString("object0"));
        assertTrue(record.getBoolean("hAshcOdEdIrtY"));
        assertEquals(NAME, record.getString("h"));
        assertEquals(RECORD.toString(), record.getString("dynObject"));
        assertEquals(INTEGERS, record.getArray(Integer.class, "array0"));
        assertEquals(STRINGS, record.getArray(String.class, "STRINGS"));
        assertEquals(LONGS, record.getArray(Long.class, "LONGS"));
        assertEquals(FLOATS, record.getArray(Float.class, "FLOATS"));
        assertEquals(DOUBLES, record.getArray(Double.class, "DOUBLES"));
        assertEquals(BOOLEANS, record.getArray(Boolean.class, "BOOLEANS"));
        assertEquals(BYTES, record.getArray(byte[].class, "BYTES"));
        assertEquals(DATES, record.getArray(ZonedDateTime.class, "DATES"));
        assertEquals(RECORDS, record.getArray(Record.class, "RECORDS"));
        record.getArray(Record.class, "RECORDS").forEach(r -> {
            assertEquals(1, r.getInt("ntgr"));
            assertEquals("one", r.getString("str"));
        });
        assertEquals(3,
                schema.getEntries().stream().filter(entry -> entry.getName().matches("hAshcOdEdIrtY|h|id")).count());
        // check we don't have any technical field in our schema/record
        assertEquals(0,
                schema
                        .getEntries()
                        .stream()
                        .filter(entry -> entry.getName().matches("hashCodeDirty|loopKey|lookKey"))
                        .count());
        assertThrows(NullPointerException.class, () -> record.getBoolean("hashCodeDirty"));
        assertNull(record.getString("loopKey"));
        assertNull(record.getString("lookKey"));
    }

    @Test
    void visitEmptyAndNullFields() {
        final RowStructEmptyNull r1 = new RowStructEmptyNull();
        r1.meta_id = 1;
        r1.FirstName = "";
        r1.lastName = "";
        r1.City = "";
        r1.i = "";
        r1.A = "";
        final RowStructEmptyNull r2 = new RowStructEmptyNull();
        r2.meta_id = 2;
        r2.FirstName = "Bob";
        r2.lastName = "Kent";
        r2.City = "London";
        r2.i = "i";
        r2.A = "A";
        final RowStructEmptyNull r3 = new RowStructEmptyNull();
        r3.meta_id = 3;
        //
        final DiRowStructVisitor visitor = new DiRowStructVisitor();
        final Record rcd1 = visitor.get(r1, factory);
        final Record rcd2 = visitor.get(r2, factory);
        final Record rcd3 = visitor.get(r3, factory);
        assertEquals(1, rcd1.getInt("meta_id"));
        assertEquals("", rcd1.getString("FirstName"));
        assertEquals("", rcd1.getString("lastName"));
        assertEquals("", rcd1.getString("City"));
        assertEquals("", rcd1.getString("i"));
        assertEquals("", rcd1.getString("A"));
        assertEquals(2, rcd2.getInt("meta_id"));
        assertEquals("Bob", rcd2.getString("FirstName"));
        assertEquals("Kent", rcd2.getString("lastName"));
        assertEquals("London", rcd2.getString("City"));
        assertEquals("i", rcd2.getString("i"));
        assertEquals("A", rcd2.getString("A"));
        assertEquals(3, rcd3.getInt("meta_id"));
        assertNull(rcd3.getString("FirstName"));
        assertNull(rcd3.getString("lastName"));
        assertNull(rcd3.getString("City"));
        assertNull(rcd3.getString("i"));
        assertNull(rcd3.getString("A"));
    }

    public static class Rcd {

        public String str = "one";

        public int ntgr = 1;
    }

    @Data
    public static class RowStructEmptyNull implements routines.system.IPersistableRow {

        public Integer meta_id;

        public String FirstName;

        public String lastName;

        public String City;

        public String i;

        public String A;

        @Override
        public void writeData(final ObjectOutputStream objectOutputStream) {
            throw new UnsupportedOperationException("#writeData()");
        }

        @Override
        public void readData(final ObjectInputStream objectInputStream) {
            throw new UnsupportedOperationException("#readData()");
        }
    }
}