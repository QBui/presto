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
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.google.common.base.Splitter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decoder for avro object.
 */
public class AvroRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data,
            String dataSchema,
            Map<String, String> dataMap,
            Set<FieldValueProvider> fieldValueProviders,
            List<DecoderColumnHandle> columnHandles,
            Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders)
    {
        GenericDatumReader<GenericRecord> datumReader;
        GenericRecord avroRecord;

        try {
            Schema schema = (new Schema.Parser()).parse(dataSchema);
            datumReader = new GenericDatumReader<>(schema);

            avroRecord = datumReader.read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(data), null));
        }
        catch (Exception e) {
            return true;
        }

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }

            @SuppressWarnings("unchecked")
            FieldDecoder<Object> decoder = (FieldDecoder<Object>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                Object element = locateElement(avroRecord, columnHandle);
                fieldValueProviders.add(decoder.decode(element, columnHandle));
            }
        }

        return false;
    }

    private Object locateElement(GenericRecord element, DecoderColumnHandle columnHandle)
    {
        Object value = element;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(columnHandle.getMapping())) {
            if (value == null) {
                return null;
            }
            value = ((GenericRecord) value).get(pathElement);
        }
        return value;
    }
}
