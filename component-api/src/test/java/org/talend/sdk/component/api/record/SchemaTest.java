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
package org.talend.sdk.component.api.record;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.json.Json;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;

import lombok.RequiredArgsConstructor;

class SchemaTest {

    @Test
    void testGetEntry() {
        final Schema sc1 = new SchemaExample(null, Collections.emptyMap());
        Assertions.assertNull(sc1.getEntry("unknown"));

        final Entry e1 = new EntryExample("e1", Collections.emptyMap());
        final Entry e2 = new EntryExample("e2", Collections.emptyMap());
        final Schema sc2 = new SchemaExample(Arrays.asList(e1, e2), Collections.emptyMap());
        Assertions.assertNull(sc2.getEntry("unknown"));
        Assertions.assertSame(e1, sc2.getEntry("e1"));
        Assertions.assertSame(e2, sc2.getEntry("e2"));
    }

    @Test
    void testJsonProp() {
        final Map<String, String> testMap = new HashMap<>(3);
        testMap.put("key1", "value1");
        testMap.put("key2", Json.createObjectBuilder().add("Hello", 5).build().toString());
        testMap.put("key3", Json.createArrayBuilder().add(1).add(2).build().toString());

        final Schema sc1 = new SchemaExample(null, testMap);
        Assertions.assertNull(sc1.getJsonProp("unexist"));

        final JsonValue value1 = sc1.getJsonProp("key1");
        Assertions.assertEquals(ValueType.STRING, value1.getValueType());
        Assertions.assertEquals("value1", ((JsonString) value1).getString());

        final JsonValue value2 = sc1.getJsonProp("key2");
        Assertions.assertEquals(ValueType.OBJECT, value2.getValueType());
        Assertions.assertEquals(5, value2.asJsonObject().getJsonNumber("Hello").intValue());

        final JsonValue value3 = sc1.getJsonProp("key3");
        Assertions.assertEquals(ValueType.ARRAY, value3.getValueType());
        Assertions.assertEquals(1, value3.asJsonArray().getJsonNumber(0).intValue());
        Assertions.assertEquals(2, value3.asJsonArray().getJsonNumber(1).intValue());
    }

    @Test
    void testJsonPropForEntry() {
        final Map<String, String> testMap = new HashMap<>(3);
        testMap.put("key1", "value1");
        testMap.put("key2", Json.createObjectBuilder().add("Hello", 5).build().toString());
        testMap.put("key3", Json.createArrayBuilder().add(1).add(2).build().toString());

        final Entry entry = new EntryExample(null, testMap);
        Assertions.assertNull(entry.getJsonProp("unexist"));

        final JsonValue value1 = entry.getJsonProp("key1");
        Assertions.assertEquals(ValueType.STRING, value1.getValueType());
        Assertions.assertEquals("value1", ((JsonString) value1).getString());

        final JsonValue value2 = entry.getJsonProp("key2");
        Assertions.assertEquals(ValueType.OBJECT, value2.getValueType());
        Assertions.assertEquals(5, value2.asJsonObject().getJsonNumber("Hello").intValue());

        final JsonValue value3 = entry.getJsonProp("key3");
        Assertions.assertEquals(ValueType.ARRAY, value3.getValueType());
        Assertions.assertEquals(1, value3.asJsonArray().getJsonNumber(0).intValue());
        Assertions.assertEquals(2, value3.asJsonArray().getJsonNumber(1).intValue());
    }

    @RequiredArgsConstructor
    class SchemaExample implements Schema {

        private final List<Entry> entries;

        private final Map<String, String> props;

        @Override
        public Type getType() {
            return Type.RECORD;
        }

        @Override
        public Schema getElementSchema() {
            return null;
        }

        @Override
        public List<Entry> getEntries() {
            return entries;
        }

        @Override
        public List<Entry> getMetadata() {
            return Collections.emptyList();
        }

        @Override
        public Stream<Entry> getAllEntries() {
            return Optional.ofNullable(this.entries).map(List::stream).orElse(Stream.empty());
        }

        @Override
        public Map<String, String> getProps() {
            return props;
        }

        @Override
        public String getProp(final String property) {
            return props.get(property);
        }

        @Override
        public Builder toBuilder() {
            return null;
        }
    }

    @RequiredArgsConstructor
    class EntryExample implements Schema.Entry {

        private final String name;

        private final Map<String, String> props;

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getRawName() {
            return null;
        }

        @Override
        public String getOriginalFieldName() {
            return null;
        }

        @Override
        public Type getType() {
            return null;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Override
        public boolean isMetadata() {
            return false;
        }

        @Override
        public <T> T getDefaultValue() {
            return null;
        }

        @Override
        public Schema getElementSchema() {
            return null;
        }

        @Override
        public String getComment() {
            return null;
        }

        @Override
        public Map<String, String> getProps() {
            return this.props;
        }

        @Override
        public String getProp(final String property) {
            return this.props.get(property);
        }

        @Override
        public Builder toBuilder() {
            return null;
        }
    }

    @Test
    void testTypes() {
        final Record rec = new Record() {

            @Override
            public Schema getSchema() {
                return null;
            }

            @Override
            public <T> T get(final Class<T> expectedType, final String name) {
                return null;
            }
        };
        Assertions.assertTrue(Type.RECORD.isCompatible(rec));
        Assertions.assertFalse(Type.RECORD.isCompatible(1234));

        Assertions.assertTrue(Type.ARRAY.isCompatible(Collections.emptyList()));
        Assertions.assertFalse(Schema.Type.ARRAY.isCompatible(new int[] {}));

        Assertions.assertTrue(Type.STRING.isCompatible("Hello"));
        Assertions.assertFalse(Schema.Type.STRING.isCompatible(new int[] {}));

        Assertions.assertTrue(Type.BYTES.isCompatible("Hello".getBytes()));
        Assertions.assertTrue(Type.BYTES.isCompatible(new Byte[] {}));
        Assertions.assertFalse(Schema.Type.BYTES.isCompatible(new int[] {}));

        Assertions.assertTrue(Schema.Type.INT.isCompatible(null));
        Assertions.assertTrue(Schema.Type.INT.isCompatible(123));
        Assertions.assertFalse(Schema.Type.INT.isCompatible(234L));
        Assertions.assertFalse(Schema.Type.INT.isCompatible("Hello"));

        Assertions.assertTrue(Schema.Type.LONG.isCompatible(123L));
        Assertions.assertFalse(Schema.Type.LONG.isCompatible(234));

        Assertions.assertTrue(Schema.Type.FLOAT.isCompatible(123.2f));
        Assertions.assertFalse(Schema.Type.FLOAT.isCompatible(234.1d));

        Assertions.assertTrue(Schema.Type.DOUBLE.isCompatible(123.2d));
        Assertions.assertFalse(Schema.Type.DOUBLE.isCompatible(Float.valueOf(3.4f)));

        Assertions.assertTrue(Schema.Type.BOOLEAN.isCompatible(Boolean.TRUE));
        Assertions.assertFalse(Schema.Type.BOOLEAN.isCompatible(10));

        Assertions.assertTrue(Type.DATETIME.isCompatible(System.currentTimeMillis()));
        Assertions.assertTrue(Type.DATETIME.isCompatible(new Date()));
        Assertions.assertTrue(Type.DATETIME.isCompatible(ZonedDateTime.now()));
        Assertions.assertFalse(Schema.Type.DATETIME.isCompatible(10));
    }

    @Test
    void testSanitize() {
        Assertions.assertNull(Schema.sanitizeConnectionName(null));
        Assertions.assertEquals("", Schema.sanitizeConnectionName(""));
        Assertions.assertEquals("_", Schema.sanitizeConnectionName("$"));
        Assertions.assertEquals("_", Schema.sanitizeConnectionName("1"));
        Assertions.assertEquals("_", Schema.sanitizeConnectionName("é"));
        Assertions.assertEquals("H", Schema.sanitizeConnectionName("éH"));
        Assertions.assertEquals("_1", Schema.sanitizeConnectionName("é1"));
        Assertions.assertEquals("H_lloWorld", Schema.sanitizeConnectionName("HélloWorld"));
        Assertions.assertEquals("oid", Schema.sanitizeConnectionName("$oid"));
        Assertions.assertEquals("Hello_World_", Schema.sanitizeConnectionName(" Hello World "));
        Assertions.assertEquals("_23HelloWorld", Schema.sanitizeConnectionName("123HelloWorld"));

        Assertions.assertEquals("Hello_World_", Schema.sanitizeConnectionName("Hello-World$"));

        final Pattern checkPattern = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
        final String nonAscii1 = Schema.sanitizeConnectionName("30_39歳");
        Assertions.assertTrue(checkPattern.matcher(nonAscii1).matches(), "'" + nonAscii1 + "' don't match");

        final String ch1 = Schema.sanitizeConnectionName("世帯数分布");
        final String ch2 = Schema.sanitizeConnectionName("抽出率調整");
        Assertions.assertTrue(checkPattern.matcher(ch1).matches(), "'" + ch1 + "' don't match");
        Assertions.assertTrue(checkPattern.matcher(ch2).matches(), "'" + ch2 + "' don't match");
        Assertions.assertNotEquals(ch1, ch2);

        final Random rnd = new Random();
        final byte[] array = new byte[20]; // length is bounded by 7
        for (int i = 0; i < 150; i++) {
            rnd.nextBytes(array);
            final String randomString = new String(array, StandardCharsets.UTF_8);
            final String sanitize = Schema.sanitizeConnectionName(randomString);
            Assertions.assertTrue(checkPattern.matcher(sanitize).matches(), "'" + sanitize + "' don't match");

            final String sanitize2 = Schema.sanitizeConnectionName(sanitize);
            Assertions.assertEquals(sanitize, sanitize2);
        }
    }

}