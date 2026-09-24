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

import java.util.HashSet;
import java.util.Set;

/**
 * Batched Destination FlowFile Filter returns matches based on common Streaming Channel
 * and atomically claims the selected destination to prevent out-of-order FlowFile processing
 */
class BatchedDestinationFlowFilter implements FlowFileFilter {
    private static final int MAXIMUM_FLOW_FILES = 1000;

    private static final StreamingDestinationProvider streamingDestinationProvider = new StreamingDestinationProvider();

    private final ProcessContext context;

    private final long maximumBytes;

    private final StreamingDestinationManager destinationManager;

    private final Set<StreamingDestination> unavailableDestinations = new HashSet<>();

    private int flowFilesAccepted;

    private long flowFileBytesAccepted;

    private StreamingDestination streamingDestination;

    BatchedDestinationFlowFilter(
            final ProcessContext context,
            final long maximumBytes,
            final StreamingDestinationManager destinationManager
    ) {
        this.context = context;
        this.maximumBytes = maximumBytes;
        this.destinationManager = destinationManager;
    }

    @Override
    public FlowFileFilterResult filter(final FlowFile flowFile) {
        final StreamingDestination flowFileStreamingDestination = streamingDestinationProvider.getStreamingDestination(context, flowFile);

        if (unavailableDestinations.contains(flowFileStreamingDestination)) {
            return FlowFileFilterResult.REJECT_AND_CONTINUE;
        }

        if (streamingDestination == null) {
            if (!destinationManager.claimDestination(flowFileStreamingDestination)) {
                unavailableDestinations.add(flowFileStreamingDestination);
                return FlowFileFilterResult.REJECT_AND_CONTINUE;
            }
            streamingDestination = flowFileStreamingDestination;
        }

        final FlowFileFilterResult filterResult;
        if (streamingDestination.equals(flowFileStreamingDestination)) {
            final long flowFileSize = flowFile.getSize();

            if (flowFileSize == 0) {
                // Empty FlowFile handling
                if (flowFilesAccepted > 0) {
                    // Reject and terminate when already accepted non-empty FlowFiles
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
