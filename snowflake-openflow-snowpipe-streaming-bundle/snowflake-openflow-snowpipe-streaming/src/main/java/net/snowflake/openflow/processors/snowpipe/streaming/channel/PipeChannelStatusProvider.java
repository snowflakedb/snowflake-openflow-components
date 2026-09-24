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
import org.apache.nifi.logging.ComponentLog;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Channel Status Provider implementation supporting distinct Provider instances for each Pipe to avoid contention
 */
public final class PipeChannelStatusProvider implements ChannelStatusProvider {

    private final ConcurrentMap<PipeReference, ChannelStatusProvider> pipeProviders = new ConcurrentHashMap<>();

    private final ComponentLog logger;

    private final StreamingChannelClient streamingChannelClient;

    private final Clock clock;

    public PipeChannelStatusProvider(
            final ComponentLog logger,
            final StreamingChannelClient streamingChannelClient,
            final Clock clock
    ) {
        this.logger = Objects.requireNonNull(logger, "Component Log required");
        this.streamingChannelClient = Objects.requireNonNull(streamingChannelClient, "Streaming Channel Client required");
        this.clock = Objects.requireNonNull(clock, "Clock required");
    }

    @Override
    public ChannelStatus getChannelStatus(final StreamingChannel streamingChannel) {
        Objects.requireNonNull(streamingChannel, "Streaming Channel required");

        final PipeReference pipeReference = new PipeReference(
                streamingChannel.getDatabase(),
                streamingChannel.getSchema(),
                streamingChannel.getPipe()
        );

        final ChannelStatusProvider pipeProvider = pipeProviders.computeIfAbsent(pipeReference,
                reference -> new SharedChannelStatusProvider(logger, streamingChannelClient, clock)
        );

        return pipeProvider.getChannelStatus(streamingChannel);
    }

    @Override
    public boolean isChannelStatusRefreshRequired(final StreamingChannel streamingChannel) {
        Objects.requireNonNull(streamingChannel, "Streaming Channel required");

        final PipeReference pipeReference = new PipeReference(
                streamingChannel.getDatabase(),
                streamingChannel.getSchema(),
                streamingChannel.getPipe()
        );

        final ChannelStatusProvider pipeProvider = pipeProviders.get(pipeReference);

        // Channel Status has not been retrieved for Pipes without a Provider
        return pipeProvider == null || pipeProvider.isChannelStatusRefreshRequired(streamingChannel);
    }

    private record PipeReference(
            String database,
            String schema,
            String pipe
    ) {

    }
}
