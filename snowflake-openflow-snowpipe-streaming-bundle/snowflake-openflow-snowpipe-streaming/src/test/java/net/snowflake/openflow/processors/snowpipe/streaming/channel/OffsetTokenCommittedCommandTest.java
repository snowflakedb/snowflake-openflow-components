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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OffsetTokenCommittedCommandTest {
    private static final String DATABASE = "SNOWFLAKE";

    private static final String SCHEMA = "OPENFLOW";

    private static final String PIPE = "STREAMING";

    private static final String FIRST_CHANNEL = "PARTITION-0";

    private static final String OFFSET_TOKEN = "1";

    private static final long ROWS_ERROR_COUNT = 10;

    private static final String PARSING_FAILED_MESSAGE = "Parsing Failed";

    private static final String CHANNEL_STATUS_MESSAGE = "Channel not found";

    private static final URI CHANNEL_STATUS_URI = URI.create("https://localhost/pipes/STREAMING:bulk-channel-status");

    private static final String REQUEST_METHOD = "GET";

    @Mock
    private ChannelStatusProvider channelStatusProvider;

    @Mock
    private ChannelStatus channelStatus;

    private StreamingChannel streamingChannel;

    private OffsetTokenCommittedCommand command;

    @BeforeEach
    void setCommand() {
        streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        streamingChannel.setOffsetToken(OFFSET_TOKEN);
        command = new OffsetTokenCommittedCommand(channelStatusProvider, streamingChannel);
    }

    @Test
    void testRunOffsetTokenNotCommitted() {
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenReturn(channelStatus);
        when(channelStatus.channelStatusCode()).thenReturn(ChannelStatusCode.SUCCESS.getStatus());
        when(channelStatus.lastCommittedOffsetToken()).thenReturn(null);

        command.run();

        assertFalse(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.VALID, command.getStreamingChannelStatus());
        assertNull(command.getLastErrorMessage());
    }

    @Test
    void testRunOffsetTokenNotCommittedInvalid() {
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenReturn(channelStatus);
        when(channelStatus.rowsErrorCount()).thenReturn(ROWS_ERROR_COUNT);
        when(channelStatus.lastErrorMessage()).thenReturn(PARSING_FAILED_MESSAGE);
        when(channelStatus.lastCommittedOffsetToken()).thenReturn(null);

        command.run();

        assertFalse(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.INVALID, command.getStreamingChannelStatus());
        assertEquals(PARSING_FAILED_MESSAGE, command.getLastErrorMessage());
        // Channel baseline starts at zero, so all reported errored rows are newly failed rows
        assertEquals(ROWS_ERROR_COUNT, command.getLastErrorCount());
    }

    @Test
    void testRunInvalidLastErrorCountReflectsNewlyFailedRows() {
        final long previousErrorCount = 5;
        final long lastErrorCountExpected = ROWS_ERROR_COUNT - previousErrorCount;
        streamingChannel.setRowsErrorCount(previousErrorCount);

        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenReturn(channelStatus);
        when(channelStatus.rowsErrorCount()).thenReturn(ROWS_ERROR_COUNT);
        when(channelStatus.lastErrorMessage()).thenReturn(PARSING_FAILED_MESSAGE);
        when(channelStatus.lastCommittedOffsetToken()).thenReturn(null);

        command.run();

        assertEquals(StreamingChannelStatus.INVALID, command.getStreamingChannelStatus());
        assertEquals(PARSING_FAILED_MESSAGE, command.getLastErrorMessage());
        // Only rows errored beyond the previous baseline count as newly failed rows
        assertEquals(lastErrorCountExpected, command.getLastErrorCount());
    }

    @Test
    void testRunOffsetTokenCommitted() {
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenReturn(channelStatus);
        when(channelStatus.channelStatusCode()).thenReturn(ChannelStatusCode.SUCCESS.getStatus());
        when(channelStatus.lastCommittedOffsetToken()).thenReturn(OFFSET_TOKEN);

        command.run();

        assertTrue(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.VALID, command.getStreamingChannelStatus());
        assertNull(command.getLastErrorMessage());
    }

    @Test
    void testRunOffsetTokenCommittedRowsErrorCountUnchanged() {
        // Channel already recorded an error count, so a status reporting the same count is not treated as new invalid rows
        streamingChannel.setRowsErrorCount(ROWS_ERROR_COUNT);

        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenReturn(channelStatus);
        when(channelStatus.channelStatusCode()).thenReturn(ChannelStatusCode.SUCCESS.getStatus());
        when(channelStatus.rowsErrorCount()).thenReturn(ROWS_ERROR_COUNT);
        when(channelStatus.lastCommittedOffsetToken()).thenReturn(OFFSET_TOKEN);

        command.run();

        assertTrue(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.VALID, command.getStreamingChannelStatus());
        assertNull(command.getLastErrorMessage());
        assertEquals(ROWS_ERROR_COUNT, streamingChannel.getRowsErrorCount());
    }

    @Test
    void testRunChannelStatusRuntimeException() {
        final IllegalStateException exception = new IllegalStateException();
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenThrow(exception);

        command.run();

        assertFalse(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.VALID, command.getStreamingChannelStatus());
        assertNull(command.getLastErrorMessage());
        assertSame(exception, command.getLastException());
    }

    @Test
    void testRunChannelStatusHttpNotFoundResponseException() {
        final HttpResponseException exception = new HttpResponseException(CHANNEL_STATUS_MESSAGE, CHANNEL_STATUS_URI, REQUEST_METHOD, HTTP_NOT_FOUND);
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenThrow(exception);

        command.run();

        assertFalse(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.FAILURE, command.getStreamingChannelStatus());

        final String lastErrorMessage = command.getLastErrorMessage();
        assertNotNull(lastErrorMessage);
        assertTrue(lastErrorMessage.contains(CHANNEL_STATUS_MESSAGE));
    }

    @Test
    void testRunChannelStatusHttpInternalErrorResponseException() {
        final HttpResponseException exception = new HttpResponseException(CHANNEL_STATUS_MESSAGE, CHANNEL_STATUS_URI, REQUEST_METHOD, HTTP_INTERNAL_ERROR);
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenThrow(exception);

        command.run();

        assertFalse(command.isOffsetTokenCommitted());
        assertEquals(StreamingChannelStatus.VALID, command.getStreamingChannelStatus());
        assertNull(command.getLastErrorMessage());
        assertSame(exception, command.getLastException());
        assertEquals(1, command.getRunCount());
    }

    @Test
    void testRunTransientFailureClearedOnSuccess() {
        final HttpResponseException exception = new HttpResponseException(CHANNEL_STATUS_MESSAGE, CHANNEL_STATUS_URI, REQUEST_METHOD, HTTP_INTERNAL_ERROR);
        when(channelStatusProvider.getChannelStatus(eq(streamingChannel))).thenThrow(exception).thenReturn(channelStatus);
        when(channelStatus.channelStatusCode()).thenReturn(ChannelStatusCode.SUCCESS.getStatus());
        when(channelStatus.lastCommittedOffsetToken()).thenReturn(null);

        command.run();
        command.run();

        assertNull(command.getLastException());
        assertEquals(2, command.getRunCount());
    }
}
