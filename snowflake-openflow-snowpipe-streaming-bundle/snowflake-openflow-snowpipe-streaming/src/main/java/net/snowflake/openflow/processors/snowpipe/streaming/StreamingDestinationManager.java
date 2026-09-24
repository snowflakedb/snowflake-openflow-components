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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the lifecycle of Streaming Destinations through CLAIMED and PENDING phases.
 * Provides thread-safe state transitions backed by a single ConcurrentHashMap with
 * atomic state changes on each DestinationEntry via AtomicReference.
 */
class StreamingDestinationManager {

    private static final DestinationState CLAIMED = new DestinationState.Claimed();

    private final ConcurrentMap<StreamingDestination, DestinationEntry> entries = new ConcurrentHashMap<>();

    /**
     * Atomically claim a destination for processing.
     * A claimed destination cannot be claimed again until released.
     *
     * @param streamingDestination Streaming Destination to claim
     * @return true when the destination was successfully claimed by the current thread
     */
    boolean claimDestination(final StreamingDestination streamingDestination) {
        Objects.requireNonNull(streamingDestination, "Streaming Destination required");
        final DestinationEntry entry = new DestinationEntry();
        return entries.putIfAbsent(streamingDestination, entry) == null;
    }

    /**
     * Atomically transition a claimed destination to pending with associated batch data.
     * The destination must have been previously claimed via {@link #claimDestination}.
     *
     * @param streamingDestination Streaming Destination with claimed status
     * @param pendingBatch Pending Batch to register
     */
    void registerPendingBatch(final StreamingDestination streamingDestination, final PendingBatch pendingBatch) {
        Objects.requireNonNull(streamingDestination, "Streaming Destination required");
        Objects.requireNonNull(pendingBatch, "Pending Batch required");

        final DestinationEntry entry = entries.get(streamingDestination);
        if (entry == null) {
            throw new IllegalStateException("Destination not claimed [%s]".formatted(streamingDestination));
        }

        final DestinationState pending = new DestinationState.Pending(pendingBatch);
        if (!entry.state.compareAndSet(CLAIMED, pending)) {
            throw new IllegalStateException("Destination not claimed [%s]".formatted(streamingDestination));
        }
    }

    /**
     * Get all claimed destinations including those in the PENDING phase
     *
     * @return Snapshot of destinations claimed and not yet released
     */
    Set<StreamingDestination> getClaimedDestinations() {
        return Set.copyOf(entries.keySet());
    }

    /**
     * Get all destinations with pending batches awaiting offset commitment
     *
     * @return Collection of destination and pending batch pairs in PENDING state
     */
    Collection<Map.Entry<StreamingDestination, PendingBatch>> getPendingDestinations() {
        final List<Map.Entry<StreamingDestination, PendingBatch>> pending = new ArrayList<>();
        for (final Map.Entry<StreamingDestination, DestinationEntry> entry : entries.entrySet()) {
            final DestinationState destinationState = entry.getValue().state.get();
            if (destinationState instanceof DestinationState.Pending(PendingBatch pendingBatch)) {
                pending.add(Map.entry(entry.getKey(), pendingBatch));
            }
        }
        return pending;
    }

    /**
     * Release a claimed or pending destination making it available for processing
     *
     * @param streamingDestination Streaming Destination to release
     */
    void releaseDestination(final StreamingDestination streamingDestination) {
        Objects.requireNonNull(streamingDestination, "Streaming Destination required");
        entries.remove(streamingDestination);
    }

    private sealed interface DestinationState {

        record Claimed() implements DestinationState {

        }

        record Pending(PendingBatch pendingBatch) implements DestinationState {

        }
    }

    private static class DestinationEntry {
        private final AtomicReference<DestinationState> state = new AtomicReference<>(CLAIMED);
    }
}
