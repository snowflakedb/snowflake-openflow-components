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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class StreamingDestinationManagerTest {

    private static final String DATABASE = "SNOWFLAKE";

    private static final String SCHEMA = "OPENFLOW";

    private static final String PIPE = "STREAMING";

    private static final String CHANNEL_GROUP = "SHARED";

    private static final StreamingDestination DESTINATION = new StreamingDestination(DATABASE, SCHEMA, PIPE, CHANNEL_GROUP);

    private static final StreamingDestination OTHER_DESTINATION = new StreamingDestination(DATABASE, SCHEMA, "OTHER_PIPE", CHANNEL_GROUP);

    @Mock
    private ProcessSession session;

    @Mock
    private StreamingChannel streamingChannel;

    @Mock
    private OffsetTokenCommittedCommand command;

    @Mock
    private FlowFile flowFile;

    private StreamingDestinationManager manager;

    @BeforeEach
    void setUp() {
        manager = new StreamingDestinationManager();
    }

    @Test
    void testClaimDestination() {
        final boolean claimed = manager.claimDestination(DESTINATION);

        assertTrue(claimed);
        assertTrue(manager.getClaimedDestinations().contains(DESTINATION));
    }

    @Test
    void testClaimDestinationAlreadyClaimed() {
        manager.claimDestination(DESTINATION);

        final boolean claimedAgain = manager.claimDestination(DESTINATION);

        assertFalse(claimedAgain);
    }

    @Test
    void testClaimDestinationMultipleDistinct() {
        final boolean firstClaimed = manager.claimDestination(DESTINATION);
        final boolean secondClaimed = manager.claimDestination(OTHER_DESTINATION);

        assertTrue(firstClaimed);
        assertTrue(secondClaimed);
        assertEquals(2, manager.getClaimedDestinations().size());
    }

    @Test
    void testClaimDestinationNull() {
        assertThrows(NullPointerException.class, () -> manager.claimDestination(null));
    }

    @Test
    void testRegisterPendingBatch() {
        manager.claimDestination(DESTINATION);

        final PendingBatch pendingBatch = newPendingBatch();
        manager.registerPendingBatch(DESTINATION, pendingBatch);

        final Collection<Map.Entry<StreamingDestination, PendingBatch>> pending = manager.getPendingDestinations();
        assertEquals(1, pending.size());

        final Map.Entry<StreamingDestination, PendingBatch> entry = pending.iterator().next();
        assertEquals(DESTINATION, entry.getKey());
        assertEquals(pendingBatch, entry.getValue());
    }

    @Test
    void testRegisterPendingBatchNotClaimed() {
        final PendingBatch pendingBatch = newPendingBatch();

        assertThrows(IllegalStateException.class, () -> manager.registerPendingBatch(DESTINATION, pendingBatch));
    }

    @Test
    void testRegisterPendingBatchNullDestination() {
        assertThrows(NullPointerException.class, () -> manager.registerPendingBatch(null, newPendingBatch()));
    }

    @Test
    void testRegisterPendingBatchNullBatch() {
        manager.claimDestination(DESTINATION);

        assertThrows(NullPointerException.class, () -> manager.registerPendingBatch(DESTINATION, null));
    }

    @Test
    void testClaimDestinationBlockedWhilePending() {
        manager.claimDestination(DESTINATION);
        manager.registerPendingBatch(DESTINATION, newPendingBatch());

        final boolean claimedAgain = manager.claimDestination(DESTINATION);

        assertFalse(claimedAgain);
    }

    @Test
    void testGetPendingDestinationsEmpty() {
        final Collection<Map.Entry<StreamingDestination, PendingBatch>> pending = manager.getPendingDestinations();

        assertTrue(pending.isEmpty());
    }

    @Test
    void testGetPendingDestinationsExcludesClaimed() {
        manager.claimDestination(DESTINATION);

        final Collection<Map.Entry<StreamingDestination, PendingBatch>> pending = manager.getPendingDestinations();

        assertTrue(pending.isEmpty());
    }

    @Test
    void testReleaseDestinationClaimed() {
        manager.claimDestination(DESTINATION);

        manager.releaseDestination(DESTINATION);

        assertTrue(manager.getClaimedDestinations().isEmpty());
    }

    @Test
    void testReleaseDestinationPending() {
        manager.claimDestination(DESTINATION);
        manager.registerPendingBatch(DESTINATION, newPendingBatch());

        manager.releaseDestination(DESTINATION);

        assertTrue(manager.getClaimedDestinations().isEmpty());
        assertTrue(manager.getPendingDestinations().isEmpty());
    }

    @Test
    void testReleaseDestinationAllowsReclaim() {
        manager.claimDestination(DESTINATION);
        manager.releaseDestination(DESTINATION);

        final boolean reclaimed = manager.claimDestination(DESTINATION);

        assertTrue(reclaimed);
    }

    @Test
    void testReleaseDestinationNotClaimed() {
        manager.releaseDestination(DESTINATION);

        assertTrue(manager.getClaimedDestinations().isEmpty());
    }

    @Test
    void testReleaseDestinationNull() {
        assertThrows(NullPointerException.class, () -> manager.releaseDestination(null));
    }

    @Test
    void testGetClaimedDestinationsIncludesBothPhases() {
        manager.claimDestination(DESTINATION);
        manager.claimDestination(OTHER_DESTINATION);
        manager.registerPendingBatch(OTHER_DESTINATION, newPendingBatch());

        final Set<StreamingDestination> claimed = manager.getClaimedDestinations();

        assertEquals(2, claimed.size());
        assertTrue(claimed.contains(DESTINATION));
        assertTrue(claimed.contains(OTHER_DESTINATION));
    }

    private PendingBatch newPendingBatch() {
        return new PendingBatch(session, List.of(flowFile), streamingChannel, DESTINATION, Instant.now(), command);
    }
}
