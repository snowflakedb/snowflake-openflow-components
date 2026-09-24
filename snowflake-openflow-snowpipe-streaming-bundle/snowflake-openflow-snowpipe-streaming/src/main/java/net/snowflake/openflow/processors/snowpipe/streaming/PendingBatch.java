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

import net.snowflake.openflow.processors.snowpipe.streaming.channel.OffsetTokenCommittedCommand;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pending Batch tracks FlowFiles sent to a Streaming Channel awaiting offset token commitment
 */
final class PendingBatch {
    private final AtomicBoolean claimed = new AtomicBoolean(false);

    private final ProcessSession session;

    private final List<FlowFile> flowFiles;

    private final StreamingChannel streamingChannel;

    private final StreamingDestination streamingDestination;

    private final Instant created;

    private final OffsetTokenCommittedCommand offsetTokenCommittedCommand;

    PendingBatch(
            final ProcessSession session,
            final List<FlowFile> flowFiles,
            final StreamingChannel streamingChannel,
            final StreamingDestination streamingDestination,
            final Instant created,
            final OffsetTokenCommittedCommand offsetTokenCommittedCommand
    ) {
        this.session = Objects.requireNonNull(session, "Process Session required");
        this.flowFiles = Objects.requireNonNull(flowFiles, "FlowFiles required");
        this.streamingChannel = Objects.requireNonNull(streamingChannel, "Streaming Channel required");
        this.streamingDestination = Objects.requireNonNull(streamingDestination, "Streaming Destination required");
        this.created = Objects.requireNonNull(created, "Created Instant required");
        this.offsetTokenCommittedCommand = Objects.requireNonNull(offsetTokenCommittedCommand, "Offset Token Committed Command required");
    }

    ProcessSession getSession() {
        return session;
    }

    List<FlowFile> getFlowFiles() {
        return flowFiles;
    }

    StreamingChannel getStreamingChannel() {
        return streamingChannel;
    }

    StreamingDestination getStreamingDestination() {
        return streamingDestination;
    }

    Instant getCreated() {
        return created;
    }

    OffsetTokenCommittedCommand getOffsetTokenCommittedCommand() {
        return offsetTokenCommittedCommand;
    }

    /**
     * Attempt to claim exclusive processing rights for this batch
     *
     * @return true when claimed for processing by the current thread
     */
    boolean tryClaim() {
        return claimed.compareAndSet(false, true);
    }

    /**
     * Release claim allowing another thread to process this batch
     */
    void releaseClaim() {
        claimed.set(false);
    }
}
