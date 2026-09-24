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

import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;

/**
 * Abstraction for providing Channel Status information from Snowpipe Streaming
 */
public interface ChannelStatusProvider {
    /**
     * Get Channel Status for specified Streaming Channel
     *
     * @param streamingChannel Streaming Channel
     * @return Channel Status
     */
    ChannelStatus getChannelStatus(StreamingChannel streamingChannel);

    /**
     * Determine whether retrieving Channel Status for the specified Streaming Channel requires a request to Snowflake
     *
     * @param streamingChannel Streaming Channel
     * @return Refresh required when cached Channel Status has passed expiration and the Pipe refresh period has elapsed
     */
    boolean isChannelStatusRefreshRequired(StreamingChannel streamingChannel);
}
