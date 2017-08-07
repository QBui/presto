/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.decoder.avro;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkIsNull;
import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestAvroDecoder
{
    private static final AvroFieldDecoder DEFAULT_FIELD_DECODER = new AvroFieldDecoder();

    private static Map<DecoderColumnHandle, FieldDecoder<?>> buildMap(List<DecoderColumnHandle> columns)
    {
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> map = ImmutableMap.builder();
        for (DecoderColumnHandle column : columns) {
            map.put(column, DEFAULT_FIELD_DECODER);
        }
        return map.build();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        User user = User.newBuilder()
                .setId(98247748L)
                .setIdStr("98247748")
                .setScreenName("EKentuckyN")
                .setStatusesCount(7630L)
                .setGeoEnabled(true)
                .build();
        Message message = Message.newBuilder()
                .setId(493857959588286460L)
                .setIdStr("493857959588286460")
                .setCreatedAt("Mon Dec 21 01:17:22 +0000 2009")
                .setSource("<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>")
                .setUser(user)
                .build();
        SpecificDatumWriter<Message> dataWriter = new SpecificDatumWriter<>(Message.getClassSchema());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(os, null);
        dataWriter.write(message, e);
        e.flush();
        byte[] data = os.toByteArray();
        os.close();

        String schemaStr = Message.getClassSchema().toString();
        AvroRowDecoder rowDecoder = new AvroRowDecoder();
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", createVarcharType(100), "source", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(10), "user/screen_name", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", BigintType.BIGINT, "id", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BigintType.BIGINT, "user/statuses_count", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle("", 4, "row5", BooleanType.BOOLEAN, "user/geo_enabled", null, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4, row5);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(data, schemaStr, null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>");
        checkValue(providers, row2, "EKentuckyN");
        checkValue(providers, row3, 493857959588286460L);
        checkValue(providers, row4, 7630);
        checkValue(providers, row5, true);
    }

    @Test
    public void testNonExistent()
            throws Exception
    {
        User user = User.newBuilder()
                .setId(null)
                .setIdStr(null)
                .setScreenName(null)
                .setStatusesCount(null)
                .setGeoEnabled(null)
                .build();
        Message message = Message.newBuilder()
                .setId(null)
                .setIdStr(null)
                .setCreatedAt(null)
                .setSource(null)
                .setUser(user)
                .build();
        SpecificDatumWriter<Message> dataWriter = new SpecificDatumWriter<>(Message.getClassSchema());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Encoder e = EncoderFactory.get().binaryEncoder(os, null);
        dataWriter.write(message, e);
        e.flush();
        byte[] data = os.toByteArray();
        os.close();

        String schemaStr = Message.getClassSchema().toString();
        AvroRowDecoder rowDecoder = new AvroRowDecoder();
        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", createVarcharType(100), "very/deep/varchar", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", BigintType.BIGINT, "no_bigint", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", DoubleType.DOUBLE, "double/is_missing", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle("", 3, "row4", BooleanType.BOOLEAN, "hello", null, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3, row4);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = rowDecoder.decodeRow(data, schemaStr, null, providers, columns, buildMap(columns));
        assertFalse(corrupt);

        assertEquals(providers.size(), columns.size());

        checkIsNull(providers, row1);
        checkIsNull(providers, row2);
        checkIsNull(providers, row3);
        checkIsNull(providers, row4);
    }
}
