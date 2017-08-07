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
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.predicate.NullableValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class KafkaPartition
{
    private final Map<ColumnHandle, NullableValue> keys;
    private final HostAddress partitionLeader;
    private final int partitionId;
    private final long offsetStart;
    private final long offsetEnd;
    private final long timestamp;

    @JsonCreator
    public KafkaPartition(
            @JsonProperty Map<ColumnHandle, NullableValue> keys,
            @JsonProperty HostAddress partitionLeader,
            @JsonProperty int partitionId,
            @JsonProperty long offsetStart,
            @JsonProperty long offsetEnd,
            @JsonProperty long timestamp
    )
    {
        this.keys = ImmutableMap.copyOf(requireNonNull(keys, "keys is null"));
        this.partitionLeader = requireNonNull(partitionLeader, "partitionLeader is null");
        this.partitionId = requireNonNull(partitionId,  "partitionLeader is null");
        this.offsetStart = requireNonNull(offsetStart,  "offsetStart is null");
        this.offsetEnd = requireNonNull(offsetEnd,  "offsetEnd is null");
        this.timestamp = requireNonNull(timestamp,  "timestamp is null");
    }

    @JsonProperty
    public Map<ColumnHandle, NullableValue> getKeys()
    {
        return keys;
    }

    @JsonProperty
    public HostAddress getPartitionLeader()
    {
        return partitionLeader;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public long getOffsetStart()
    {
        return offsetStart;
    }

    @JsonProperty
    public long getOffsetEnd()
    {
        return offsetEnd;
    }

    @JsonProperty
    public long getTimestamp()
    {
        return timestamp;
    }
}
