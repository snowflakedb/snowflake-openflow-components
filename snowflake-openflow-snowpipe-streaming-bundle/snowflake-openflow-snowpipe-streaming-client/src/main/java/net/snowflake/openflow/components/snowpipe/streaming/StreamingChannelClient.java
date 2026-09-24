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

package net.snowflake.openflow.components.snowpipe.streaming;

import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelNames;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.InsertStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.OpenedChannel;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeInfo;

import java.util.List;
import java.util.UUID;

/**
 * Snowpipe Streaming Channel Client for status and transfer operations
 */
public interface StreamingChannelClient {
    /**
     * Get Bulk Channel Status for specified Channel Names
     *
     * @param database Database Name
     * @param schema Schema Name
     * @param pipe Pipe Name
     * @param bulkChannelNames Channel Names to be checked
     * @param requestId Request Identifier for correlated tracking
     * @return Bulk Channel Status
     */
    BulkChannelStatus getBulkChannelStatus(String database, String schema, String pipe, BulkChannelNames bulkChannelNames, UUID requestId);

    /**
     * Get Pipe Information with Stage Location and Storage Credentials
     *
     * @param database Database Name
     * @param schema Schema Name
     * @param pipe Pipe Name
     * @param requestId Request Identifier for correlated tracking
     * @return Pipe Information
     */
    PipeInfo getPipeInfo(String database, String schema, String pipe, UUID requestId);

    /**
     * Open Channel for specified destination
     *
     * @param database Database Name
     * @param schema Schema Name
     * @param pipe Pipe Name
     * @param channel Channel Name to be opened
     * @param requestId Request Identifier for correlated tracking
     * @return Opened Channel status
     */
    OpenedChannel openChannel(String database, String schema, String pipe, String channel, UUID requestId);

    /**
     * Insert Rows of Newline Delimited JSON to specified Channel with required Continuation Token and optional Offset Token
     *
     * @param database Database Name
     * @param schema Schema Name
     * @param pipe Pipe Name
     * @param channel Channel Name
     * @param continuationToken Optional Continuation Token from opened Channel matching expected remote sequencers
     * @param offsetToken Optional Offset Token for subsequent polling and tracking using Channel Status
     * @param rows Newline Delimited JSON with zstd compression applied
     * @param uncompressedContentLength Length in bytes of uncompressed rows
     * @param requestId Request Identifier for correlated tracking
     * @return Insert Status
     */
    InsertStatus insertRows(
            String database,
            String schema,
            String pipe,
            String channel,
            String continuationToken,
            String offsetToken,
            byte[] rows,
            long uncompressedContentLength,
            UUID requestId
    );

    /**
     * Insert File Fragments referencing Storage Locations to specified Channel with required Continuation Token
     *
     * @param database Database Name
     * @param schema Schema Name
     * @param pipe Pipe Name
     * @param channel Channel Name
     * @param continuationToken Optional Continuation Token from opened Channel matching expected remote sequencers
     * @param fileFragments File Fragment Information referencing Storage Location of Newline Delimited JSON
     * @param requestId Request Identifier for correlated tracking
     * @return Insert Status
     */
    InsertStatus insertFileFragments(
            String database,
            String schema,
            String pipe,
            String channel,
            String continuationToken,
            List<FileFragmentInfo> fileFragments,
            UUID requestId
    );
}
