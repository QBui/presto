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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertLayout;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaSimpleConsumerManager consumerManager;
    private final Set<HostAddress> nodes;

    @Inject
    public KafkaSplitManager(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaSimpleConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.nodes = ImmutableSet.copyOf(kafkaConnectorConfig.getNodes());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        KafkaTableLayoutHandle layoutHandle = (KafkaTableLayoutHandle) layout;

        List<KafkaPartition> partitions = layoutHandle.getPartitions();
        ImmutableList<ConnectorSplit> splits = partitions.stream()
                .filter(partition -> partition.getOffsetStart() < partition.getOffsetEnd()) // Split end is exclusive.
                .map(partition -> new KafkaSplit(
                                connectorId,
                                kafkaTableHandle.getTopicName(),
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getKeyDataSchema(),
                                kafkaTableHandle.getMessageDataFormat(),
                                kafkaTableHandle.getMessageDataSchema(),
                                partition.getPartitionId(),
                                partition.getOffsetStart(),
                                partition.getOffsetEnd(),
                                partition.getPartitionLeader(),
                                partition.getTimestamp()))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }
}
