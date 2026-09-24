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

package net.snowflake.openflow.processors.snowpipe.streaming;

import net.snowflake.openflow.processors.snowpipe.streaming.channel.ChannelStatusProvider;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * Service encapsulating Snowpipe Streaming channel management and data transfer operations
 */
interface SnowpipeStreamingService extends Closeable {
    /**
     * Close and release all resources
     *
     */
    @Override
    void close();

    /**
     * Borrow a Streaming Channel from the pool for the given destination
     *
     * @param streamingDestination Streaming Destination for channel allocation
     * @return Optional containing the borrowed channel, or empty when unavailable
     */
    Optional<StreamingChannel> borrowChannel(StreamingDestination streamingDestination);

    /**
     * Return a Streaming Channel to the pool
     *
     * @param streamingChannel Streaming Channel to return
     * @param streamingDestination Streaming Destination associated with the channel
     */
    void returnChannel(StreamingChannel streamingChannel, StreamingDestination streamingDestination);

    /**
     * Invalidate a Streaming Channel removing it from the pool
     *
     * @param streamingChannel Streaming Channel to invalidate
     * @param streamingDestination Streaming Destination associated with the channel
     */
    void invalidateChannel(StreamingChannel streamingChannel, StreamingDestination streamingDestination);

    /**
     * Transfer FlowFiles to Snowpipe Streaming through the given channel with buffering, compression, and insert operations
     *
     * @param context Process Context for property evaluation
     * @param session Process Session for FlowFile content access and counter adjustments
     * @param flowFiles FlowFiles to transfer
     * @param streamingChannel Streaming Channel for data transfer
     */
    void transferFlowFiles(ProcessContext context, ProcessSession session, List<FlowFile> flowFiles, StreamingChannel streamingChannel);

    /**
     * Get the Channel Status Provider for offset token commitment tracking
     *
     * @return Channel Status Provider
     */
    ChannelStatusProvider getChannelStatusProvider();

    /**
     * Get the Transit URI for provenance reporting
     *
     * @param streamingChannel Streaming Channel for URI construction
     * @return Transit URI string
     */
    String getTransitUri(StreamingChannel streamingChannel);
}
