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

import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;

/**
 * Streaming Destination FlowFile Filter returns matches based on common Streaming Channel
 */
class StreamingDestinationFlowFileFilter implements FlowFileFilter {
    // Maximum number of FlowFiles accepted
    private static final int MAXIMUM_FLOW_FILES = 1000;

    private final ProcessContext context;

    private int flowFilesAccepted;

    private long flowFileBytesAccepted;

    private final long maximumBytes;

    private final StreamingDestinationProvider streamingDestinationProvider = new StreamingDestinationProvider();

    private StreamingDestination streamingDestination;

    StreamingDestinationFlowFileFilter(final ProcessContext context, final long maximumBytes) {
        this.context = context;
        this.maximumBytes = maximumBytes;
    }

    @Override
    public FlowFileFilterResult filter(final FlowFile flowFile) {
        final StreamingDestination flowFileStreamingDestination = streamingDestinationProvider.getStreamingDestination(context, flowFile);
        if (streamingDestination == null) {
            streamingDestination = flowFileStreamingDestination;
        }

        final FlowFileFilterResult filterResult;
        if (streamingDestination.equals(flowFileStreamingDestination)) {
            final long flowFileSize = flowFile.getSize();

            if (flowFileSize == 0) {
                // Empty FlowFile handling
                if (flowFilesAccepted > 0) {
                    // Reject and terminate if we've already accepted non-empty FlowFiles
                    filterResult = FlowFileFilterResult.REJECT_AND_TERMINATE;
                } else {
                    // Accept and terminate first empty FlowFile so it can be routed to EMPTY relationship
                    filterResult = FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                }
            } else if (flowFileSize >= maximumBytes) {
                // Accept one FlowFile when larger than maximum number of bytes
                filterResult = FlowFileFilterResult.ACCEPT_AND_TERMINATE;
            } else {
                flowFilesAccepted++;
                flowFileBytesAccepted += flowFileSize;

                if (flowFileBytesAccepted >= maximumBytes) {
                    // Reject FlowFile and terminate filtering when exceeding maximum number of bytes
                    filterResult = FlowFileFilterResult.REJECT_AND_TERMINATE;
                } else if (flowFilesAccepted == MAXIMUM_FLOW_FILES) {
                    // Accept FlowFile and terminate filtering when reaching maximum number of FlowFiles
                    filterResult = FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                } else {
                    filterResult = FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                }
            }
        } else {
            filterResult = FlowFileFilterResult.REJECT_AND_CONTINUE;
        }

        return filterResult;
    }

    StreamingDestination getStreamingDestination() {
        return streamingDestination;
    }
}
