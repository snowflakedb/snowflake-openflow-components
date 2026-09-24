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

import com.github.luben.zstd.Zstd;
import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.InsertStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.OpenedChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.ChannelStatusProvider;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ChannelType;
import net.snowflake.openflow.processors.snowpipe.streaming.property.OffsetTrackingResolution;
import net.snowflake.openflow.processors.snowpipe.streaming.property.TransferStrategy;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.web.client.api.WebClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(5)
@ExtendWith(MockitoExtension.class)
class StandardSnowpipeStreamingServiceTest {

    private static final String DATABASE = "OPENFLOW";

    private static final String SCHEMA = "RUNTIME";

    private static final String PIPE = "SNOWPIPE";

    private static final String CHANNEL_NAME = "SHARED-0";

    private static final String CHANNEL_GROUP = "SHARED";

    private static final String CONTINUATION_TOKEN = "0_1";

    private static final String NEXT_CONTINUATION_TOKEN = "0_2";

    private static final String OFFSET_TOKEN = "1";

    private static final String NDJSON_ROW = "{\"ID\":1}\n";

    private static final long NDJSON_ROW_LENGTH = NDJSON_ROW.length();

    private static final URI STREAMING_URI = URI.create("https://streaming.snowflakecomputing.com");

    private static final StreamingDestination DESTINATION = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

    private static final Duration CHANNEL_INSERT_TIMEOUT = Duration.ofSeconds(5);

    private static final int MAXIMUM_BUFFER_SIZE = 1024;

    private static final ChannelStatus CHANNEL_STATUS = new ChannelStatus(
            ChannelStatusCode.SUCCESS.getStatus(),
            null,
            System.currentTimeMillis(),
            DATABASE,
            SCHEMA,
            PIPE,
            CHANNEL_NAME,
            0,
            0,
            0,
            0,
            null,
            0,
            null,
            null,
            0
    );

    private static final OpenedChannel OPENED_CHANNEL = new OpenedChannel(CONTINUATION_TOKEN, CHANNEL_STATUS);

    @Mock
    private ComponentLog logger;

    @Mock
    private WebClientService webClientService;

    @Mock
    private StreamingChannelClient streamingChannelClient;

    @Mock
    private ProcessContext context;

    @Mock
    private ProcessSession session;

    @Mock
    private FlowFile flowFile;

    private StandardSnowpipeStreamingService service;

    @BeforeEach
    void setUp() {
        service = createService(OffsetTrackingResolution.FLOW_FILE);
    }

    @Test
    void testBorrowChannelSuccess() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final Optional<StreamingChannel> result = service.borrowChannel(DESTINATION);

        assertTrue(result.isPresent());
        final StreamingChannel channel = result.get();
        assertEquals(DATABASE, channel.getDatabase());
        assertEquals(SCHEMA, channel.getSchema());
        assertEquals(PIPE, channel.getPipe());
        assertEquals(CHANNEL_NAME, channel.getChannel());
        assertEquals(CONTINUATION_TOKEN, channel.getContinuationToken());
    }

    @Test
    void testBorrowChannelOpenFailed() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenThrow(new RuntimeException("Connection refused"));

        final Optional<StreamingChannel> result = service.borrowChannel(DESTINATION);

        assertTrue(result.isEmpty());
    }

    @Test
    void testReturnChannelBorrowAgain() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final StreamingChannel channel = service.borrowChannel(DESTINATION).orElseThrow();
        service.returnChannel(channel, DESTINATION);

        final Optional<StreamingChannel> secondBorrow = service.borrowChannel(DESTINATION);

        assertTrue(secondBorrow.isPresent());
        assertSame(channel, secondBorrow.get());
        verify(streamingChannelClient).openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class));
    }

    @Test
    void testInvalidateChannelNewChannelCreated() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final StreamingChannel channel = service.borrowChannel(DESTINATION).orElseThrow();
        service.invalidateChannel(channel, DESTINATION);

        final Optional<StreamingChannel> secondBorrow = service.borrowChannel(DESTINATION);

        assertTrue(secondBorrow.isPresent());
        assertNotSame(channel, secondBorrow.get());
    }

    @Test
    void testClosePoolClosed() {
        service.close();

        final Optional<StreamingChannel> result = service.borrowChannel(DESTINATION);

        assertTrue(result.isEmpty());
    }

    @Test
    void testGetTransitUri() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final StreamingChannel channel = service.borrowChannel(DESTINATION).orElseThrow();
        final String transitUri = service.getTransitUri(channel);

        final String expected = "https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s"
                .formatted(STREAMING_URI.getHost(), DATABASE, SCHEMA, PIPE, CHANNEL_NAME);
        assertEquals(expected, transitUri);
    }

    @Test
    void testGetChannelStatusProvider() {
        final ChannelStatusProvider provider = service.getChannelStatusProvider();

        assertNotNull(provider);
    }

    @Test
    void testTransferFlowFilesCompressed() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final StreamingChannel channel = service.borrowChannel(DESTINATION).orElseThrow();

        final InsertStatus insertStatus = new InsertStatus(0, null, NEXT_CONTINUATION_TOKEN);
        when(streamingChannelClient.insertRows(
                eq(DATABASE), eq(SCHEMA), eq(PIPE), eq(CHANNEL_NAME),
                eq(CONTINUATION_TOKEN), eq(OFFSET_TOKEN),
                any(byte[].class), eq((long) NDJSON_ROW.length()), any(UUID.class)
        )).thenReturn(insertStatus);

        setOffsetTokenEndExpression();

        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);

        final byte[] compressedBytes = Zstd.compress(NDJSON_ROW.getBytes(StandardCharsets.UTF_8));
        when(session.read(flowFile)).thenReturn(new ByteArrayInputStream(compressedBytes));
        when(session.getProvenanceReporter()).thenReturn(provenanceReporter);

        service.transferFlowFiles(context, session, List.of(flowFile), channel);

        verifyInsertRows(channel, provenanceReporter);
    }

    @Test
    void testTransferFlowFilesInsertRows() {
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final StreamingChannel channel = service.borrowChannel(DESTINATION).orElseThrow();

        final InsertStatus insertStatus = new InsertStatus(0, null, NEXT_CONTINUATION_TOKEN);
        when(streamingChannelClient.insertRows(
                eq(DATABASE), eq(SCHEMA), eq(PIPE), eq(CHANNEL_NAME),
                eq(CONTINUATION_TOKEN), eq(OFFSET_TOKEN),
                any(byte[].class), eq((long) NDJSON_ROW.length()), any(UUID.class)
        )).thenReturn(insertStatus);

        setOffsetTokenEndExpression();

        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);
        when(session.read(flowFile)).thenReturn(new ByteArrayInputStream(NDJSON_ROW.getBytes(StandardCharsets.UTF_8)));
        when(session.getProvenanceReporter()).thenReturn(provenanceReporter);

        service.transferFlowFiles(context, session, List.of(flowFile), channel);

        verifyInsertRows(channel, provenanceReporter);
    }

    @Test
    void testTransferFlowFilesCommittedSkipped() {
        final ChannelStatus committedStatus = new ChannelStatus(
                ChannelStatusCode.SUCCESS.getStatus(),
                "10",
                System.currentTimeMillis(),
                DATABASE,
                SCHEMA,
                PIPE,
                CHANNEL_NAME,
                0,
                0,
                0,
                0,
                null,
                0,
                null,
                null,
                0
        );
        final OpenedChannel committedChannel = new OpenedChannel(CONTINUATION_TOKEN, committedStatus);
        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(committedChannel);

        final StreamingChannel channel = service.borrowChannel(DESTINATION).orElseThrow();

        setOffsetTokenEndExpression();

        service.transferFlowFiles(context, session, List.of(flowFile), channel);

        verify(streamingChannelClient, never()).insertRows(any(), any(), any(), any(), any(), any(), any(), anyLong(), any());
        verify(session).adjustCounter(eq(StandardSnowpipeStreamingService.SessionCounter.FLOW_FILES_SKIPPED.counter), eq(1L), eq(false));
    }

    @Test
    void testTransferFlowFilesOffsetTrackingResolutionDisabled() {
        final StandardSnowpipeStreamingService streamingService = createService(OffsetTrackingResolution.DISABLED);

        when(
                streamingChannelClient.openChannel(eq(DATABASE), eq(SCHEMA), eq(PIPE), anyString(), any(UUID.class))
        ).thenReturn(OPENED_CHANNEL);

        final StreamingChannel channel = streamingService.borrowChannel(DESTINATION).orElseThrow();

        final InsertStatus insertStatus = new InsertStatus(0, null, NEXT_CONTINUATION_TOKEN);
        when(
                streamingChannelClient.insertRows(
                        eq(DATABASE),
                        eq(SCHEMA),
                        eq(PIPE),
                        eq(CHANNEL_NAME),
                        eq(CONTINUATION_TOKEN),
                        anyString(),
                        any(byte[].class),
                        eq(NDJSON_ROW_LENGTH),
                        any(UUID.class)
                )
        ).thenReturn(insertStatus);

        final ProvenanceReporter provenanceReporter = mock(ProvenanceReporter.class);
        when(session.read(flowFile)).thenReturn(new ByteArrayInputStream(NDJSON_ROW.getBytes(StandardCharsets.UTF_8)));
        when(session.getProvenanceReporter()).thenReturn(provenanceReporter);

        streamingService.transferFlowFiles(context, session, List.of(flowFile), channel);

        verify(streamingChannelClient).insertRows(
                eq(DATABASE),
                eq(SCHEMA),
                eq(PIPE),
                eq(CHANNEL_NAME),
                eq(CONTINUATION_TOKEN),
                anyString(),
                any(byte[].class),
                eq(NDJSON_ROW_LENGTH),
                any(UUID.class)
        );
        assertEquals(NEXT_CONTINUATION_TOKEN, channel.getContinuationToken());

        streamingService.close();
    }

    private void verifyInsertRows(final StreamingChannel channel, final ProvenanceReporter provenanceReporter) {
        verify(streamingChannelClient).insertRows(
                eq(DATABASE), eq(SCHEMA), eq(PIPE), eq(CHANNEL_NAME),
                eq(CONTINUATION_TOKEN), eq(OFFSET_TOKEN),
                any(byte[].class), eq(NDJSON_ROW_LENGTH), any(UUID.class)
        );
        assertEquals(NEXT_CONTINUATION_TOKEN, channel.getContinuationToken());
        assertEquals(OFFSET_TOKEN, channel.getOffsetToken());

        verify(session).adjustCounter(eq(StandardSnowpipeStreamingService.SessionCounter.ROWS_SENT.counter), eq(1L), eq(false));
        verify(session).adjustCounter(eq(StandardSnowpipeStreamingService.SessionCounter.BATCHES_SENT.counter), eq(1L), eq(false));
        verify(provenanceReporter).send(eq(flowFile), anyString(), eq(STREAMING_URI.getHost()), anyLong());
    }

    private StandardSnowpipeStreamingService createService(final OffsetTrackingResolution offsetTrackingResolution) {
        return new StandardSnowpipeStreamingService(
                logger,
                STREAMING_URI,
                webClientService,
                streamingChannelClient,
                TransferStrategy.ROWS,
                ChannelType.STANDARD,
                offsetTrackingResolution,
                CHANNEL_INSERT_TIMEOUT,
                null,
                MAXIMUM_BUFFER_SIZE
        );
    }

    private void setOffsetTokenEndExpression() {
        final PropertyValue endOffsetPropertyValue = mock(PropertyValue.class);
        final PropertyValue evaluatedEndOffset = mock(PropertyValue.class);
        when(context.getProperty(PublishSnowpipeStreaming.OFFSET_TOKEN_END_EXPRESSION)).thenReturn(endOffsetPropertyValue);
        when(endOffsetPropertyValue.evaluateAttributeExpressions(flowFile)).thenReturn(evaluatedEndOffset);
        when(evaluatedEndOffset.getValue()).thenReturn(OFFSET_TOKEN);
    }
}
