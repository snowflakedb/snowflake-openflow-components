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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.util.List;

/**
 * Defines the FlowFile attribute that reports the number of row errors added to the Channel when a transfer
 * completes and provides shared logic for applying it, ensuring a single authoritative definition across the
 * publishing processors. The transfer outcome itself is conveyed through the destination relationship.
 */
final class ChannelStatusAttributes {

    static final String CHANNEL_STATUS_ROW_ERRORS_ADDED = "channel.status.row.errors.added";

    private ChannelStatusAttributes() {
    }

    /**
     * Applies the row errors added attribute to each of the supplied FlowFiles.
     *
     * @param session        the process session used to update the FlowFiles
     * @param flowFiles      the FlowFiles to tag
     * @param rowErrorsAdded the number of row errors added to the Channel for the transfer
     * @return the updated FlowFiles carrying the row errors added attribute
     */
    static List<FlowFile> apply(
            final ProcessSession session,
            final List<FlowFile> flowFiles,
            final long rowErrorsAdded
    ) {
        return flowFiles.stream()
                .map(flowFile -> session.putAttribute(flowFile, CHANNEL_STATUS_ROW_ERRORS_ADDED, Long.toString(rowErrorsAdded)))
                .toList();
    }
}
