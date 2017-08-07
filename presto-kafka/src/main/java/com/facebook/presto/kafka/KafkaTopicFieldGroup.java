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
package com.facebook.presto.kafka;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.VarcharType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Groups the field descriptions for message or key.
 */
public class KafkaTopicFieldGroup
{
    private final String dataFormat;
    private final String dataSchema;
    private final Boolean dynamic;
    private final List<KafkaTopicFieldDescription> fields;

    @JsonCreator
    public KafkaTopicFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("dataSchema") String dataSchema,
            @JsonProperty("dynamic") Boolean dynamic,
            @JsonProperty("fields") List<KafkaTopicFieldDescription> fields)
    {
        // #TODO dynamic schema only for avro or protobuf
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.dynamic = dynamic == null ? false : dynamic;
        if (!isDynamic()) {
            this.dataSchema = dataSchema;
            this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
        }
        else {
            // dataSchema is required if use dynamic schema
            this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
            List<KafkaTopicFieldDescription> newFields = new ArrayList<>();
            try {
                Class<?> clazz = Class.forName(getDataSchema());
                Method getDescriptorMethod = clazz.getDeclaredMethod("getDescriptor");
                Descriptors.Descriptor descriptor = (Descriptors.Descriptor) getDescriptorMethod.invoke(null);
                generateFields(newFields, descriptor, "", "");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            this.fields = ImmutableList.copyOf(newFields);
        }
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public String getDataSchema()
    {
        return dataSchema;
    }

    @JsonProperty
    public boolean isDynamic()
    {
        return dynamic;
    }

    @JsonProperty
    public List<KafkaTopicFieldDescription> getFields()
    {
        return fields;
    }

    private void generateFields(List<KafkaTopicFieldDescription> fields, Descriptors.Descriptor descriptor, String namePrefix, String mappingPrefix)
    {
        for (Descriptors.FieldDescriptor fd : descriptor.getFields()) {
            String name = (namePrefix.isEmpty() ? namePrefix : namePrefix + "_") + fd.getName();
            String mapping = (mappingPrefix.isEmpty() ? mappingPrefix : mappingPrefix + "/") + fd.getName();
            switch (fd.getJavaType()) {
                case LONG:
                    fields.add(new KafkaTopicFieldDescription(name, BigintType.BIGINT, mapping, "", null, "", false));
                    break;
                case BOOLEAN:
                    fields.add(new KafkaTopicFieldDescription(name, BooleanType.BOOLEAN, mapping, "", null, "", false));
                    break;
                case STRING:
                    fields.add(new KafkaTopicFieldDescription(name, VarcharType.VARCHAR, mapping, "", null, "", false));
                    break;
                case MESSAGE:
                    // Do not support recursive data structure
                    if (fd.isRepeated() || fd.getMessageType() == descriptor) {
                        fields.add(new KafkaTopicFieldDescription(name, VarcharType.VARCHAR, mapping, "", null, "", false));
                    }
                    else {
                        generateFields(fields, fd.getMessageType(), name, mapping);
                    }
                    break;
                default:
                    fields.add(new KafkaTopicFieldDescription(name, VarcharType.VARCHAR, mapping, "", null, "", false));
                    break;
            }
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("fields", fields)
                .toString();
    }
}
