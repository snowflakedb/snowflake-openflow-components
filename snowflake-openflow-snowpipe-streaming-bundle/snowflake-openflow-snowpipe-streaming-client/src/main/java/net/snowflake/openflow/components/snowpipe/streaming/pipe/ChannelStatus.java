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

package net.snowflake.openflow.components.snowpipe.streaming.pipe;

/**
 * Channel Status
 *
 * @param channelStatusCode Status Code of SUCCESS or other string for error conditions
 * @param lastCommittedOffsetToken Last Committed Offset Token or null for new Channels
 * @param createdOnMs Channel created in epoch milliseconds
 * @param databaseName Database Name
 * @param schemaName Schema Name
 * @param pipeName Pipe Name
 * @param channelName Channel Name
 * @param rowsInserted Count of rows inserted since Channel creation
 * @param rowsParsed Count of rows parsed since Channel creation
 * @param rowsErrorCount Count of rows errored since Channel creation
 * @param statusCode Status Code as a number
 * @param message Status message or null on success
 * @param lastErrorTimestamp Timestamp of last error during processing
 * @param lastErrorOffsetUpperBound Offset Token of last error during processing
 * @param lastErrorMessage Message of last error during processing
 * @param snowflakeAvgProcessingLatencyMs Average duration of Snowflake processing latency since Channel creation
 */
public record ChannelStatus(
        String channelStatusCode,
        String lastCommittedOffsetToken,
        long createdOnMs,
        String databaseName,
        String schemaName,
        String pipeName,
        String channelName,
        long rowsInserted,
        long rowsParsed,
        long rowsErrorCount,
        long statusCode,
        String message,
        long lastErrorTimestamp,
        String lastErrorOffsetUpperBound,
        String lastErrorMessage,
        long snowflakeAvgProcessingLatencyMs
) {
}
