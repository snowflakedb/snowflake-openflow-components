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

import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.VerifiableProcessor;

import java.net.URI;

/**
 * Abstraction for configuring Streaming Channel Client and associated properties
 */
public interface StreamingClientProvider extends VerifiableProcessor {
    /**
     * Get Snowflake Account URI based on provided configuration properties
     *
     * @param context Process Context
     * @return Snowflake Account URI
     */
    URI getAccountUri(ProcessContext context);

    /**
     * Get Snowpipe Streaming URI based on provided configuration properties
     *
     * @param context Process Context
     * @return Snowpipe Streaming URI
     */
    URI getStreamingUri(ProcessContext context);

    /**
     * Get Streaming Channel Client based on provided configuration and Snowpipe Streaming URI
     *
     * @param context Process Context
     * @param streamingUri Snowpipe Streaming URI
     * @return Streaming Channel Client
     */
    StreamingChannelClient getStreamingChannelClient(ProcessContext context, URI streamingUri);
}
