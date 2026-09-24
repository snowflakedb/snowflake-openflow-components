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
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamingDestinationFlowFileFilterTest {

    private static final String DATABASE = "TEST_DB";

    private static final String SCHEMA = "TEST_SCHEMA";

    private static final String PIPE = "TEST_PIPE";

    private static final String CHANNEL_GROUP = "TEST_GROUP";

    private static final StreamingDestination DESTINATION = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(PublishSnowpipeStreaming.class);
        runner.setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.PIPE);
        runner.setProperty(StreamingDestinationProvider.DATABASE, DATABASE);
        runner.setProperty(StreamingDestinationProvider.SCHEMA, SCHEMA);
        runner.setProperty(StreamingDestinationProvider.PIPE, PIPE);
        runner.setProperty(StreamingDestinationProvider.CHANNEL_GROUP, CHANNEL_GROUP);
    }

    @Test
    void testFilterAcceptAndContinueNonEmptyFlowFile() {
        final ProcessContext context = runner.getProcessContext();

        final StreamingDestinationFlowFileFilter filter = new StreamingDestinationFlowFileFilter(context, Long.MAX_VALUE);

        final byte[] data = new byte[1024];
        final MockFlowFile flowFile = new MockFlowFile(Long.MAX_VALUE);
        flowFile.setData(data);

        final FlowFileFilter.FlowFileFilterResult result = filter.filter(flowFile);

        assertEquals(FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_CONTINUE, result);

        final StreamingDestination streamingDestination = filter.getStreamingDestination();
        assertEquals(DESTINATION, streamingDestination);
    }

    @Test
    void testFilterAcceptAndTerminateEmptyFlowFile() {
        final ProcessContext context = runner.getProcessContext();

        final StreamingDestinationFlowFileFilter filter = new StreamingDestinationFlowFileFilter(context, Long.MAX_VALUE);

        final MockFlowFile flowFile = new MockFlowFile(Long.MAX_VALUE);

        final FlowFileFilter.FlowFileFilterResult result = filter.filter(flowFile);

        assertEquals(FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE, result);

        final StreamingDestination streamingDestination = filter.getStreamingDestination();
        assertEquals(DESTINATION, streamingDestination);
    }

    @Test
    void testFilterAcceptMaximumBytesExceededSingleFlowFile() {
        final ProcessContext context = runner.getProcessContext();

        final byte[] data = new byte[Short.MAX_VALUE];
        final StreamingDestinationFlowFileFilter filter = new StreamingDestinationFlowFileFilter(context, Byte.MAX_VALUE);

        final MockFlowFile flowFile = new MockFlowFile(Long.MAX_VALUE);
        flowFile.setData(data);

        final FlowFileFilter.FlowFileFilterResult result = filter.filter(flowFile);

        assertEquals(FlowFileFilter.FlowFileFilterResult.ACCEPT_AND_TERMINATE, result);

        final StreamingDestination streamingDestination = filter.getStreamingDestination();
        assertEquals(DESTINATION, streamingDestination);
    }
}
