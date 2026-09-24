/*
 * Copyright 2026 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
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

package net.snowflake.openflow.processors.snowpipe.streaming.channel;

import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.OpenedChannel;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.nifi.logging.ComponentLog;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pooled Object Factory for creating and managing Streaming Channels
 */
public class StreamingChannelFactory extends BaseKeyedPooledObjectFactory<StreamingDestination, StreamingChannel> {

    private static final String CHANNEL_NAME_FORMAT = "%s-%d";

    private final ConcurrentMap<StreamingDestination, AtomicInteger> channelNumbers = new ConcurrentHashMap<>();

    private final ComponentLog logger;

    private final StreamingChannelClient streamingChannelClient;

    public StreamingChannelFactory(final ComponentLog logger, final StreamingChannelClient streamingChannelClient) {
        this.logger = logger;
        this.streamingChannelClient = streamingChannelClient;
    }

    @Override
    public StreamingChannel create(final StreamingDestination streamingDestination) {
        final AtomicInteger channelNumber = channelNumbers.computeIfAbsent(streamingDestination, destination -> new AtomicInteger());

        final UUID requestId = UUID.randomUUID();
        final OpenedChannel openedChannel;
        try {
            openedChannel = openChannel(streamingDestination, channelNumber, requestId);
        } catch (final Exception e) {
            // Decrement Channel Number on failure to open channel
            channelNumber.decrementAndGet();
            throw e;
        }

        final ChannelStatus channelStatus = openedChannel.channelStatus();
        final String channelStatusCode = channelStatus.channelStatusCode();
        if (ChannelStatusCode.SUCCESS.getStatus().equals(channelStatusCode)) {
            final String nextContinuationToken = openedChannel.nextContinuationToken();

            final StreamingChannel streamingChannel = new StreamingChannel(channelStatus.databaseName(), channelStatus.schemaName(), channelStatus.pipeName(), channelStatus.channelName());
            streamingChannel.setContinuationToken(nextContinuationToken);
            final String committedOffsetToken = channelStatus.lastCommittedOffsetToken();
            streamingChannel.setCommittedOffsetToken(committedOffsetToken);
            final long rowsErrorCount = channelStatus.rowsErrorCount();
            streamingChannel.setRowsErrorCount(rowsErrorCount);

            // Set local Offset Token to Last Committed Offset Token from Channel Status
            streamingChannel.setOffsetToken(committedOffsetToken);

            logger.info("{} Opened with Next Continuation Token [{}] Committed Offset Token [{}] Request ID [{}]", streamingChannel, nextContinuationToken, committedOffsetToken, requestId);
            return streamingChannel;
        } else {
            // Decrement Channel Number on invalid channel status
            channelNumber.decrementAndGet();
            throw new IllegalStateException("Channel [%s] invalid status [%s] Request ID [%s]".formatted(channelStatus.channelName(), channelStatusCode, requestId));
        }
    }

    @Override
    public PooledObject<StreamingChannel> wrap(final StreamingChannel streamingChannel) {
        return new DefaultPooledObject<>(streamingChannel);
    }

    @Override
    public void destroyObject(final StreamingDestination streamingDestination, final PooledObject<StreamingChannel> pooledObject) {
        final StreamingChannel streamingChannel = pooledObject.getObject();
        final String continuationToken = streamingChannel.getContinuationToken();
        logger.info("{} Removed with Continuation Token [{}]", streamingChannel, continuationToken, streamingDestination);
        channelNumbers.get(streamingDestination).decrementAndGet();
    }

    private OpenedChannel openChannel(final StreamingDestination streamingDestination, final AtomicInteger channelNumber, final UUID requestId) {
        final String channelGroup = streamingDestination.channelGroup();
        final String channel = CHANNEL_NAME_FORMAT.formatted(channelGroup, channelNumber.getAndIncrement());

        final String database = streamingDestination.database();
        final String schema = streamingDestination.schema();
        final String pipe = streamingDestination.pipe();

        return streamingChannelClient.openChannel(database, schema, pipe, channel, requestId);
    }
}
