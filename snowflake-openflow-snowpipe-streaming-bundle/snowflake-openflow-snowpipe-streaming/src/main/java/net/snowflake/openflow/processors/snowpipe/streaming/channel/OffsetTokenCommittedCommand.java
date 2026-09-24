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

import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;

import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Offset Token Committed Command retrieves Channel Status and compares Offset Token against Last Committed
 */
public final class OffsetTokenCommittedCommand implements Runnable {
    private static final String STATUS_CODE_MESSAGE_FORMAT = "Channel Status Code [%s]";

    private static final Set<Integer> FAILURE_STATUS_CODES = Set.of(
            HttpURLConnection.HTTP_BAD_REQUEST,
            HttpURLConnection.HTTP_UNAUTHORIZED,
            HttpURLConnection.HTTP_FORBIDDEN,
            HttpURLConnection.HTTP_NOT_FOUND,
            HttpURLConnection.HTTP_CONFLICT,
            HttpURLConnection.HTTP_GONE
    );

    private final ChannelStatusProvider channelStatusProvider;

    private final StreamingChannel streamingChannel;

    private final BigDecimal offsetToken;

    private final AtomicBoolean offsetTokenCommited = new AtomicBoolean();

    private final AtomicReference<StreamingChannelStatus> currentStatus = new AtomicReference<>(StreamingChannelStatus.VALID);

    private String lastErrorMessage;

    private final AtomicLong lastErrorCount = new AtomicLong();

    private final AtomicInteger runCount = new AtomicInteger();

    private Exception lastException;

    public OffsetTokenCommittedCommand(
            final ChannelStatusProvider channelStatusProvider,
            final StreamingChannel streamingChannel
    ) {
        this.channelStatusProvider = Objects.requireNonNull(channelStatusProvider, "Channel Status Provider required");
        this.streamingChannel = Objects.requireNonNull(streamingChannel, "Streaming Channel required");

        final String channelOffsetToken = streamingChannel.getOffsetToken();
        Objects.requireNonNull(channelOffsetToken, "Streaming Channel Offset Token required");
        this.offsetToken = new BigDecimal(channelOffsetToken);
    }

    @Override
    public void run() {
        runCount.incrementAndGet();
        try {
            final ChannelStatus channelStatus = channelStatusProvider.getChannelStatus(streamingChannel);
            processChannelStatus(channelStatus);
            lastException = null;
        } catch (final HttpResponseException e) {
            final int statusCode = e.getStatusCode();

            // Set current Channel Status based on selected HTTP Status Codes indicating Channel failures
            if (FAILURE_STATUS_CODES.contains(statusCode)) {
                currentStatus.set(StreamingChannelStatus.FAILURE);
                lastErrorMessage = e.getMessage();
            } else {
                lastException = e;
            }
        } catch (final Exception e) {
            lastException = e;
        }
    }

    /**
     * Get Streaming Channel Status based on last Channel Status retrieved
     *
     * @return Current Streaming Channel Status
     */
    public StreamingChannelStatus getStreamingChannelStatus() {
        return currentStatus.get();
    }

    /**
     * Get Last Error Message from Channel Status when row error count found
     *
     * @return Last Error Message or null on success
     */
    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    /**
     * Get count of rows that failed since the last observed error count when the Channel Status indicates invalid rows
     *
     * @return Number of newly failed rows or 0 when no new row errors found
     */
    public long getLastErrorCount() {
        return lastErrorCount.get();
    }

    /**
     * Get the most recent transient failure encountered while retrieving Channel Status
     *
     * @return Last transient failure or null when the latest run succeeded
     */
    public Exception getLastException() {
        return lastException;
    }

    public int getRunCount() {
        return runCount.get();
    }

    /**
     * Return Offset Token Committed status based on last Channel Status retrieved
     *
     * @return Offset Token Committed status
     */
    public boolean isOffsetTokenCommitted() {
        return offsetTokenCommited.get();
    }

    private void processChannelStatus(final ChannelStatus channelStatus) {
        final boolean committed;

        final String lastCommittedOffsetToken = channelStatus.lastCommittedOffsetToken();
        if (lastCommittedOffsetToken == null || lastCommittedOffsetToken.isEmpty()) {
            committed = false;
        } else {
            // Set Committed Offset Token for subsequent Streaming Channel evaluation and comparison of input FlowFiles
            streamingChannel.setCommittedOffsetToken(lastCommittedOffsetToken);

            final BigDecimal committedOffsetToken = new BigDecimal(lastCommittedOffsetToken);
            committed = committedOffsetToken.compareTo(offsetToken) >= 0;
        }

        final StreamingChannelStatus streamingChannelStatus = getStreamingChannelStatus(channelStatus);
        currentStatus.set(streamingChannelStatus);
        offsetTokenCommited.getAndSet(committed);
    }

    private StreamingChannelStatus getStreamingChannelStatus(final ChannelStatus channelStatus) {
        final StreamingChannelStatus streamingChannelStatus;

        final long rowsErrorCount = channelStatus.rowsErrorCount();
        if (rowsErrorCount == streamingChannel.getRowsErrorCount()) {
            final String channelStatusCode = channelStatus.channelStatusCode();
            if (ChannelStatusCode.SUCCESS.getStatus().equals(channelStatusCode)) {
                streamingChannelStatus = StreamingChannelStatus.VALID;
            } else {
                streamingChannelStatus = StreamingChannelStatus.FAILURE;
                lastErrorMessage = STATUS_CODE_MESSAGE_FORMAT.formatted(channelStatusCode);
            }
        } else {
            // Changes to rows errored indicates the invalid status
            streamingChannelStatus = StreamingChannelStatus.INVALID;
            // Record newly failed rows as the difference from the last observed error count before updating the baseline
            lastErrorCount.set(rowsErrorCount - streamingChannel.getRowsErrorCount());
            streamingChannel.setRowsErrorCount(rowsErrorCount);
            lastErrorMessage = channelStatus.lastErrorMessage();
        }
        return streamingChannelStatus;
    }
}
