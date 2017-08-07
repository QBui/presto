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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KafkaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final KafkaTableHandle table;
    private final List<ColumnHandle> partitionColumns;
    private final List<KafkaPartition> partitions;

    @JsonCreator
    public KafkaTableLayoutHandle(
            @JsonProperty("table") KafkaTableHandle table,
            @JsonProperty("partitionColumns") List<ColumnHandle> partitionColumns)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = null;
    }

    public KafkaTableLayoutHandle(
            KafkaTableHandle table,
            List<ColumnHandle> partitionColumns,
            List<KafkaPartition> partitions)
    {
        this.table = requireNonNull(table, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = partitions;
    }

    @JsonProperty
    public KafkaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public List<ColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public List<KafkaPartition> getPartitions()
    {
        return partitions;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
