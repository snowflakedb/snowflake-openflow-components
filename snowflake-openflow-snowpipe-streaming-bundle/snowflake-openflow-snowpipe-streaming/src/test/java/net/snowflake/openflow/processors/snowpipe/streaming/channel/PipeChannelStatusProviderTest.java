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
import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.processors.snowpipe.streaming.PublishSnowpipeStreaming;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipeChannelStatusProviderTest {

    private static final String DATABASE = "SNOWFLAKE";

    private static final String SCHEMA = "OPENFLOW";

    private static final String PIPE_A = "STREAMING_A";

    private static final String PIPE_B = "STREAMING_B";

    private static final String FIRST_CHANNEL = "PARTITION-0";

    private final MockComponentLog componentLog = TestRunners.newTestRunner(PublishSnowpipeStreaming.class).getLogger();

    @Mock
    private StreamingChannelClient streamingChannelClient;

    @Mock
    private ChannelStatus channelStatusA;

    @Mock
    private ChannelStatus channelStatusB;

    @Test
    void testGetChannelStatus() {
        final Clock clock = Clock.systemDefaultZone();
        final PipeChannelStatusProvider provider = new PipeChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE_A, FIRST_CHANNEL);
        when(channelStatusA.channelName()).thenReturn(FIRST_CHANNEL);
        setBulkChannelStatus(PIPE_A, channelStatusA);

        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);

        assertNotNull(channelStatus);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());
    }

    @Test
    void testGetChannelStatusDifferentPipes() {
        final Clock clock = Clock.systemDefaultZone();
        final PipeChannelStatusProvider provider = new PipeChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannelA = new StreamingChannel(DATABASE, SCHEMA, PIPE_A, FIRST_CHANNEL);
        setBulkChannelStatus(PIPE_A, channelStatusA);

        final ChannelStatus resultA = provider.getChannelStatus(streamingChannelA);
        assertNotNull(resultA);
        assertEquals(channelStatusA, resultA);

        final StreamingChannel streamingChannelB = new StreamingChannel(DATABASE, SCHEMA, PIPE_B, FIRST_CHANNEL);
        setBulkChannelStatus(PIPE_B, channelStatusB);

        final ChannelStatus resultB = provider.getChannelStatus(streamingChannelB);
        assertNotNull(resultB);
        assertEquals(channelStatusB, resultB);

        verify(streamingChannelClient, times(1)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE_A), any(), isA(UUID.class));
        verify(streamingChannelClient, times(1)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE_B), any(), isA(UUID.class));
    }

    @Test
    void testIsChannelStatusRefreshRequiredDifferentPipes() {
        final Clock clock = Clock.systemDefaultZone();
        final PipeChannelStatusProvider provider = new PipeChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannelA = new StreamingChannel(DATABASE, SCHEMA, PIPE_A, FIRST_CHANNEL);
        setBulkChannelStatus(PIPE_A, channelStatusA);
        provider.getChannelStatus(streamingChannelA);

        assertFalse(provider.isChannelStatusRefreshRequired(streamingChannelA));

        final StreamingChannel streamingChannelB = new StreamingChannel(DATABASE, SCHEMA, PIPE_B, FIRST_CHANNEL);
        assertTrue(provider.isChannelStatusRefreshRequired(streamingChannelB));

        verify(streamingChannelClient, times(1)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE_A), any(), isA(UUID.class));
        verify(streamingChannelClient, never()).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE_B), any(), isA(UUID.class));
    }

    private void setBulkChannelStatus(final String pipe, final ChannelStatus channelStatus) {
        final Map<String, ChannelStatus> channelStatuses = Map.of(FIRST_CHANNEL, channelStatus);
        final BulkChannelStatus bulkChannelStatus = new BulkChannelStatus(channelStatuses);
        when(streamingChannelClient.getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(pipe), any(), isA(UUID.class))).thenReturn(bulkChannelStatus);
    }
}
