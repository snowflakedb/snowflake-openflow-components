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
import net.snowflake.openflow.processors.snowpipe.streaming.property.DestinationType;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE;
import static org.apache.nifi.processor.FlowFileFilter.FlowFileFilterResult.REJECT_AND_CONTINUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class BatchedDestinationFlowFilterTest {

    private static final String DATABASE = "TEST_DB";
    private static final String DATABASE_B = "TEST_DB_B";
    private static final String DATABASE_C = "TEST_DB_C";
    private static final String SCHEMA = "TEST_SCHEMA";
    private static final String PIPE = "TEST_PIPE";
    private static final String CHANNEL_GROUP = "TEST_GROUP";

    private static final String TABLE = "TEST_TABLE";
    private static final String TABLE_PIPE_EXPECTED = "TEST_TABLE-STREAMING";
    private static final String TABLE_QUOTED = "\"QUOTED\"";
    private static final String TABLE_QUOTED_PIPE_EXPECTED = "\"QUOTED-STREAMING\"";
    private static final String TABLE_EXPRESSION = "${table}";

    private static final StreamingDestination DESTINATION = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);
    private static final StreamingDestination DESTINATION_B = new StreamingDestination(DATABASE_B, SCHEMA, PIPE, CHANNEL_GROUP);
    private static final StreamingDestination DESTINATION_C = new StreamingDestination(DATABASE_C, SCHEMA, PIPE, CHANNEL_GROUP);

    private static final byte[] DATA = new byte[1024];

    private static final int FILTER_PASSES = 10;

    private final AtomicLong flowFileId = new AtomicLong();

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(PublishSnowpipeStreaming.class);
        runner.setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.PIPE);
        runner.setProperty(StreamingDestinationProvider.DATABASE, "${database}");
        runner.setProperty(StreamingDestinationProvider.SCHEMA, "${schema}");
        runner.setProperty(StreamingDestinationProvider.PIPE, "${pipe}");
        runner.setProperty(StreamingDestinationProvider.CHANNEL_GROUP, "${channelGroup}");
    }

    @Test
    void testFilterAcceptAndContinueNonEmptyFlowFile() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = new StreamingDestinationManager();

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        final MockFlowFile flowFile = createFlowFile(DATA, DATABASE);

        assertEquals(ACCEPT_AND_CONTINUE, filter.filter(flowFile));
        assertNotNull(filter.getStreamingDestination());
        assertEquals(DESTINATION, filter.getStreamingDestination());
    }

    @Test
    void testFilterAcceptAndTerminateEmptyFlowFile() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = new StreamingDestinationManager();

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        final MockFlowFile flowFile = createFlowFile(new byte[0], DATABASE);

        assertEquals(ACCEPT_AND_TERMINATE, filter.filter(flowFile));
        assertNotNull(filter.getStreamingDestination());
    }

    @Test
    void testFilterRejectClaimedDestination() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = createManagerWithClaimed(DESTINATION);

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertNull(filter.getStreamingDestination());
    }

    @Test
    void testFilterAcceptMaximumBytesExceededSingleFlowFile() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = new StreamingDestinationManager();

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Byte.MAX_VALUE, manager);

        final MockFlowFile flowFile = createFlowFile(new byte[Short.MAX_VALUE], DATABASE);

        assertEquals(ACCEPT_AND_TERMINATE, filter.filter(flowFile));
        assertNotNull(filter.getStreamingDestination());
    }

    @Test
    void testFilterRejectDifferentDestination() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = new StreamingDestinationManager();

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        assertEquals(ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE_B)));
    }

    @Test
    void testFilterRejectAllFlowFilesForClaimedDestination() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = createManagerWithClaimed(DESTINATION);

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));

        assertNull(filter.getStreamingDestination());
    }

    @Test
    void testFilterClaimFailsFallsThroughToNextDestination() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = createManagerWithClaimed(DESTINATION);

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE_B)));

        assertNotNull(filter.getStreamingDestination());
        assertEquals(DESTINATION_B, filter.getStreamingDestination());

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
    }

    @Test
    void testUnavailableDestinationRemainsUnavailableThroughoutFilterPass() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = createManagerWithClaimed(DESTINATION);

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        for (int i = 0; i < FILTER_PASSES; i++) {
            assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        }

        assertNull(filter.getStreamingDestination());
    }

    @Test
    void testUnavailableDestinationStaysUnavailableEvenAfterClaimReleased() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = createManagerWithClaimed(DESTINATION);

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));

        manager.releaseDestination(DESTINATION);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));

        assertNull(filter.getStreamingDestination());
    }

    @Test
    void testRaceConditionWithMultipleDestinations() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = createManagerWithClaimed(DESTINATION, DESTINATION_B);

        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE_B)));

        manager.releaseDestination(DESTINATION);
        manager.releaseDestination(DESTINATION_B);

        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));
        assertEquals(REJECT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE_B)));
        assertEquals(ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE_C)));

        assertNotNull(filter.getStreamingDestination());
        assertEquals(DESTINATION_C, filter.getStreamingDestination());
    }

    @Test
    void testFilterDestinationTypeTable() {
        runner.setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.TABLE);
        runner.setProperty(StreamingDestinationProvider.TABLE, TABLE_EXPRESSION);

        final BatchedDestinationFlowFilter filter = getFilter();

        assertEquals(ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));

        final StreamingDestination streamingDestination = filter.getStreamingDestination();
        assertNotNull(streamingDestination);

        assertEquals(TABLE_PIPE_EXPECTED, streamingDestination.pipe());
    }

    @Test
    void testFilterDestinationTypeTableQuoted() {
        runner.setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.TABLE);
        runner.setProperty(StreamingDestinationProvider.TABLE, TABLE_QUOTED);

        final BatchedDestinationFlowFilter filter = getFilter();

        assertEquals(ACCEPT_AND_CONTINUE, filter.filter(createFlowFile(DATA, DATABASE)));

        final StreamingDestination streamingDestination = filter.getStreamingDestination();
        assertNotNull(streamingDestination);

        assertEquals(TABLE_QUOTED_PIPE_EXPECTED, streamingDestination.pipe());
    }

    private BatchedDestinationFlowFilter getFilter() {
        final ProcessContext context = runner.getProcessContext();
        final StreamingDestinationManager manager = new StreamingDestinationManager();
        return new BatchedDestinationFlowFilter(context, Long.MAX_VALUE, manager);
    }

    private MockFlowFile createFlowFile(final byte[] data, final String database) {
        final MockFlowFile flowFile = new MockFlowFile(flowFileId.getAndIncrement());
        flowFile.setData(data);
        flowFile.putAttributes(Map.of(
                "database", database,
                "schema", SCHEMA,
                "pipe", PIPE,
                "channelGroup", CHANNEL_GROUP,
                "table", TABLE
        ));
        return flowFile;
    }

    private StreamingDestinationManager createManagerWithClaimed(final StreamingDestination... destinations) {
        final StreamingDestinationManager manager = new StreamingDestinationManager();
        for (final StreamingDestination destination : destinations) {
            manager.claimDestination(destination);
        }
        return manager;
    }
}
