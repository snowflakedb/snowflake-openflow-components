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

import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.OpenedChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.PublishSnowpipeStreaming;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamingChannelFactoryTest {

    private static final String DATABASE = "SNOWFLAKE";

    private static final String SCHEMA = "OPENFLOW";

    private static final String PIPE = "STREAMING";

    private static final String CHANNEL_GROUP = "SHARED";

    private static final String FIRST_CHANNEL = "SHARED-0";

    private static final String FIRST_CONTINUATION_TOKEN = "0_1";

    private static final String INVALID_STATUS_CODE = "INVALID";

    private static final long ROWS_ERROR_COUNT = 10;

    @Mock
    private StreamingChannelClient streamingChannelClient;

    @Mock
    private OpenedChannel openedChannel;

    @Mock
    private ChannelStatus channelStatus;

    private StreamingChannelFactory factory;

    @BeforeEach
    void setFactory() {
        final TestRunner runner = TestRunners.newTestRunner(PublishSnowpipeStreaming.class);
        final MockComponentLog componentLog = runner.getLogger();

        factory = new StreamingChannelFactory(componentLog, streamingChannelClient);
    }

    @Test
    void testCreateSuccess() {
        final StreamingDestination streamingDestination = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

        setOpenedChannel();
        setChannelStatusSuccess();
        when(openedChannel.nextContinuationToken()).thenReturn(FIRST_CONTINUATION_TOKEN);

        final StreamingChannel streamingChannel = factory.create(streamingDestination);
        assertStreamingChannelCreated(streamingChannel);
    }

    @Test
    void testCreateSuccessRowsErrorCount() {
        final StreamingDestination streamingDestination = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

        setOpenedChannel();
        setChannelStatusSuccess();
        when(openedChannel.nextContinuationToken()).thenReturn(FIRST_CONTINUATION_TOKEN);
        when(channelStatus.rowsErrorCount()).thenReturn(ROWS_ERROR_COUNT);

        final StreamingChannel streamingChannel = factory.create(streamingDestination);
        assertStreamingChannelCreated(streamingChannel);
        assertEquals(ROWS_ERROR_COUNT, streamingChannel.getRowsErrorCount());
    }

    @Test
    void testCreateInvalidStatus() {
        final StreamingDestination streamingDestination = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

        setOpenedChannel();
        when(channelStatus.channelStatusCode()).thenReturn(INVALID_STATUS_CODE);

        assertThrows(IllegalStateException.class, () -> factory.create(streamingDestination));
    }

    @Test
    void testCreateInvalidStatusChannelNumberReused() {
        final StreamingDestination streamingDestination = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

        setOpenedChannel();
        when(channelStatus.channelStatusCode()).thenReturn(INVALID_STATUS_CODE);

        assertThrows(IllegalStateException.class, () -> factory.create(streamingDestination));

        setChannelStatusSuccess();
        when(openedChannel.nextContinuationToken()).thenReturn(FIRST_CONTINUATION_TOKEN);

        final StreamingChannel streamingChannel = factory.create(streamingDestination);
        assertStreamingChannelCreated(streamingChannel);
    }

    @Test
    void testCreateOpenChannelExceptionNumberReused() {
        final StreamingDestination streamingDestination = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

        when(streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), eq(FIRST_CHANNEL), isA(UUID.class))).thenThrow(new RuntimeException());

        assertThrows(RuntimeException.class, () -> factory.create(streamingDestination));

        setOpenedChannel();
        setChannelStatusSuccess();
        when(openedChannel.nextContinuationToken()).thenReturn(FIRST_CONTINUATION_TOKEN);

        final StreamingChannel streamingChannel = factory.create(streamingDestination);
        assertStreamingChannelCreated(streamingChannel);
    }

    private void assertStreamingChannelCreated(final StreamingChannel streamingChannel) {
        assertNotNull(streamingChannel);
        assertEquals(FIRST_CHANNEL, streamingChannel.getChannel());
        assertEquals(FIRST_CONTINUATION_TOKEN, streamingChannel.getContinuationToken());
    }

    private void setOpenedChannel() {
        when(streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), eq(FIRST_CHANNEL), isA(UUID.class))).thenReturn(openedChannel);
        when(openedChannel.channelStatus()).thenReturn(channelStatus);
    }

    private void setChannelStatusSuccess() {
        when(channelStatus.channelStatusCode()).thenReturn(ChannelStatusCode.SUCCESS.getStatus());
        when(channelStatus.databaseName()).thenReturn(DATABASE);
        when(channelStatus.schemaName()).thenReturn(SCHEMA);
        when(channelStatus.pipeName()).thenReturn(PIPE);
        when(channelStatus.channelName()).thenReturn(FIRST_CHANNEL);
    }
}
