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

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;

public final class KafkaUtil
{
    private static final Logger log = Logger.get(KafkaUtil.class);

    private KafkaUtil()
    {
    }

    public static long[] findAllOffsets(SimpleConsumer consumer, String topicName, int partitionId)
    {
        return findOffsets(consumer, topicName, partitionId, kafka.api.OffsetRequest.LatestTime());
    }

    public static long[] findOffsets(SimpleConsumer consumer, String topicName, int partitionId, long time)
    {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionId);

        // The API implies that this will always return all of the offsets. So it seems a partition can not have
        // more than Integer.MAX_VALUE-1 segments.
        //
        // This also assumes that the lowest value returned will be the first segment available. So if segments have been dropped off, this value
        // should not be 0.
        PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(time, Integer.MAX_VALUE);
        OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
        OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

        if (offsetResponse.hasError()) {
            short errorCode = offsetResponse.errorCode(topicName, partitionId);
            log.warn("Offset response has error: %d", errorCode);
            throw new PrestoException(KAFKA_SPLIT_ERROR, "could not fetch data from Kafka, error code is '" + errorCode + "'");
        }

        return offsetResponse.offsets(topicName, partitionId);
    }

    public static <T> T selectRandom(Iterable<T> iterable)
    {
        List<T> list = ImmutableList.copyOf(iterable);
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }
}
