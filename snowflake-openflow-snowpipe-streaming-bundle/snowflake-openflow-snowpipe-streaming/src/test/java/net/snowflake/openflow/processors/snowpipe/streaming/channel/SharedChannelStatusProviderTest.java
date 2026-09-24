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
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;
import net.snowflake.openflow.processors.snowpipe.streaming.PublishSnowpipeStreaming;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;

import static java.time.ZoneOffset.UTC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SharedChannelStatusProviderTest {

    private static final String DATABASE = "SNOWFLAKE";

    private static final String SCHEMA = "OPENFLOW";

    private static final String PIPE = "STREAMING";

    private static final String FIRST_CHANNEL = "PARTITION-0";

    private static final String SECOND_CHANNEL = "PARTITION-1";

    private static final Duration TRACKING_EXPIRATION = Duration.ofMinutes(10);

    private static final Duration CHANNEL_STATUS_EXPIRATION = Duration.ofSeconds(2);

    private static final long ROWS_ERROR_COUNT = 139;

    private final MockComponentLog componentLog = TestRunners.newTestRunner(PublishSnowpipeStreaming.class).getLogger();

    @Mock
    private StreamingChannelClient streamingChannelClient;

    @Mock
    private ChannelStatus channelStatus;

    @Mock
    private ChannelStatus secondChannelStatus;

    @Test
    void testIsChannelStatusRefreshRequired() {
        final Clock clock = mock(Clock.class);
        final Instant started = Instant.now();
        when(clock.instant()).thenReturn(started);

        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);
        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);

        assertTrue(provider.isChannelStatusRefreshRequired(streamingChannel));

        setBulkChannelStatus();
        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());
        assertFalse(provider.isChannelStatusRefreshRequired(streamingChannel));

        when(clock.instant()).thenReturn(started.plus(CHANNEL_STATUS_EXPIRATION).plusMillis(1));
        assertTrue(provider.isChannelStatusRefreshRequired(streamingChannel));
    }

    @Test
    void testGetChannelStatus() {
        final Clock clock = Clock.systemDefaultZone();
        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        setBulkChannelStatus();

        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);

        assertNotNull(channelStatus);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());
    }

    @Test
    void testGetChannelStatusCached() {
        final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        setBulkChannelStatus();

        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);

        assertNotNull(channelStatus);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());

        final ChannelStatus cachedChannelStatus = provider.getChannelStatus(streamingChannel);
        assertEquals(channelStatus, cachedChannelStatus);

        verify(streamingChannelClient, times(1)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class));
    }

    @Test
    void testGetChannelStatusCachedBeforeSharedExpiration() {
        final Clock clock = mock(Clock.class);
        final Instant started = Instant.now();
        when(clock.instant()).thenReturn(started);
        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        setBulkChannelStatus();

        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());

        // Get Channel Status for Second Channel before shared expiration has passed avoiding subsequent Bulk Channel Status
        final StreamingChannel secondStreamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, SECOND_CHANNEL);
        final ChannelStatus secondChannelStatus = provider.getChannelStatus(secondStreamingChannel);
        assertEquals(SECOND_CHANNEL, secondChannelStatus.channelName());
        assertEquals(ChannelStatusCode.SUCCESS.getStatus(), secondChannelStatus.channelStatusCode());

        verify(streamingChannelClient, times(1)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class));
    }

    @Test
    void testGetChannelStatusCachedExpired() {
        final Clock clock = mock(Clock.class);
        final Instant started = Instant.now();
        when(clock.instant()).thenReturn(started);

        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        setBulkChannelStatus();

        when(clock.instant()).thenReturn(started);
        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);

        assertNotNull(channelStatus);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());

        when(clock.instant()).thenReturn(Instant.now().plus(TRACKING_EXPIRATION));
        final ChannelStatus cachedChannelStatus = provider.getChannelStatus(streamingChannel);
        assertEquals(channelStatus, cachedChannelStatus);

        verify(streamingChannelClient, times(2)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class));
    }

    @Test
    void testGetChannelStatusMultipleChannels() {
        final Clock clock = mock(Clock.class);
        final Instant started = Instant.now();
        when(clock.instant()).thenReturn(started);
        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        when(channelStatus.channelName()).thenReturn(FIRST_CHANNEL);
        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        final Map<String, ChannelStatus> channelStatuses = Map.of(
                FIRST_CHANNEL, channelStatus
        );
        final BulkChannelStatus bulkChannelStatus = new BulkChannelStatus(channelStatuses);
        when(streamingChannelClient.getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class))).thenReturn(bulkChannelStatus);

        final ChannelStatus firstChannelStatus = provider.getChannelStatus(streamingChannel);
        assertEquals(channelStatus, firstChannelStatus);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());

        when(secondChannelStatus.channelName()).thenReturn(SECOND_CHANNEL);
        final StreamingChannel secondStreamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, SECOND_CHANNEL);
        final Map<String, ChannelStatus> secondChannelStatuses = Map.of(
                SECOND_CHANNEL, secondChannelStatus
        );
        final BulkChannelStatus secondBulkChannelStatus = new BulkChannelStatus(secondChannelStatuses);
        when(streamingChannelClient.getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class))).thenReturn(secondBulkChannelStatus);

        when(clock.instant()).thenReturn(Instant.now().plus(TRACKING_EXPIRATION));
        final ChannelStatus providedChannelStatus = provider.getChannelStatus(secondStreamingChannel);
        assertEquals(secondChannelStatus, providedChannelStatus);
        assertEquals(SECOND_CHANNEL, secondChannelStatus.channelName());

        verify(streamingChannelClient, times(2)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class));
    }

    @Test
    void testGetInitialChannelStatusPreservesRowsErrorCount() {
        final Instant started = Instant.parse("2025-01-15T10:30:00Z");
        final Clock clock = Clock.fixed(started, UTC);
        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);
        setBulkChannelStatus();
        final ChannelStatus firstChannelStatus = provider.getChannelStatus(streamingChannel);
        assertEquals(FIRST_CHANNEL, firstChannelStatus.channelName());

        // Second Channel has accumulated errors and no cached status, so the initial status is returned within the shared window
        final StreamingChannel secondStreamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, SECOND_CHANNEL);
        secondStreamingChannel.setRowsErrorCount(ROWS_ERROR_COUNT);

        final ChannelStatus initialChannelStatus = provider.getChannelStatus(secondStreamingChannel);

        assertEquals(SECOND_CHANNEL, initialChannelStatus.channelName());
        assertEquals(ChannelStatusCode.SUCCESS.getStatus(), initialChannelStatus.channelStatusCode());
        // Initial status reflects the Channel known error count to avoid a false delta and spurious invalid determination
        assertEquals(ROWS_ERROR_COUNT, initialChannelStatus.rowsErrorCount());
        assertEquals(started.toEpochMilli(), initialChannelStatus.createdOnMs());

        verify(streamingChannelClient, times(1)).getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class));
    }

    @Test
    void testGetChannelStatusChannelNotFound() {
        final Clock clock = Clock.systemDefaultZone();
        final SharedChannelStatusProvider provider = new SharedChannelStatusProvider(componentLog, streamingChannelClient, clock);

        final StreamingChannel streamingChannel = new StreamingChannel(DATABASE, SCHEMA, PIPE, FIRST_CHANNEL);

        final BulkChannelStatus bulkChannelStatus = new BulkChannelStatus(Map.of());
        when(streamingChannelClient.getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class))).thenReturn(bulkChannelStatus);

        final ChannelStatus channelStatus = provider.getChannelStatus(streamingChannel);

        assertNotNull(channelStatus);
        assertEquals(FIRST_CHANNEL, channelStatus.channelName());
        assertEquals(ChannelStatusCode.ERR_CHANNEL_NO_LONGER_EXISTS.getStatus(), channelStatus.channelStatusCode());
        assertNotNull(channelStatus.lastErrorMessage());
        assertNull(channelStatus.lastCommittedOffsetToken());
    }

    private void setBulkChannelStatus() {
        when(channelStatus.channelName()).thenReturn(FIRST_CHANNEL);
        final Map<String, ChannelStatus> channelStatuses = Map.of(FIRST_CHANNEL, channelStatus);
        final BulkChannelStatus bulkChannelStatus = new BulkChannelStatus(channelStatuses);
        when(streamingChannelClient.getBulkChannelStatus(eq(DATABASE), eq(SCHEMA), eq(PIPE), any(), isA(UUID.class))).thenReturn(bulkChannelStatus);
    }
}
