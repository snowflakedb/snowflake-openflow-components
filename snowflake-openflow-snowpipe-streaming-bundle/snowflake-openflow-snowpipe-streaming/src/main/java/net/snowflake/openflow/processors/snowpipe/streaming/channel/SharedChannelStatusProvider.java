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
import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelNames;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;
import org.apache.nifi.logging.ComponentLog;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared implementation of Channel Status Provider using Bulk Channel Status retrieval and caching
 */
public final class SharedChannelStatusProvider implements ChannelStatusProvider {
    /** Expiration Period for caching individual Channel Status */
    private static final Duration EXPIRATION_PERIOD = Duration.ofSeconds(2);

    /** Tracking Period for maintaining Channel References included in Bulk Channel Status requests */
    private static final Duration TRACKING_PERIOD = Duration.ofMinutes(5);

    private static final String CHANNEL_NOT_FOUND_MESSAGE = "Channel Status not found";

    private static final long CHANNEL_NOT_FOUND_STATUS_CODE = 404;

    private static final int EMPTY_COUNT = 0;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<ChannelReference, TrackedChannelStatus> channelReferences = new HashMap<>();

    private final Set<ChannelReference> requestedChannelReferences = ConcurrentHashMap.newKeySet();

    private final AtomicLong expiration = new AtomicLong();

    private final ComponentLog logger;

    private final StreamingChannelClient streamingChannelClient;

    private final Clock clock;

    public SharedChannelStatusProvider(
            final ComponentLog logger,
            final StreamingChannelClient streamingChannelClient,
            final Clock clock
    ) {
        this.logger = Objects.requireNonNull(logger, "Component Log required");
        this.streamingChannelClient = Objects.requireNonNull(streamingChannelClient, "Streaming Channel Client required");
        this.clock = Objects.requireNonNull(clock, "Clock required");
        this.expiration.set(clock.instant().toEpochMilli());
    }

    @Override
    public ChannelStatus getChannelStatus(final StreamingChannel streamingChannel) {
        Objects.requireNonNull(streamingChannel, "Streaming Channel required");

        final ChannelReference channelReference = new ChannelReference(
                streamingChannel.getDatabase(),
                streamingChannel.getSchema(),
                streamingChannel.getPipe(),
                streamingChannel.getChannel()
        );

        final ChannelStatus channelStatus;

        final TrackedChannelStatus cachedTrackedChannelStatus = getCachedTrackedChannelStatus(channelReference);
        if (isChannelRefreshRequired(cachedTrackedChannelStatus)) {
            if (isRefreshRequired()) {
                channelStatus = getCurrentChannelStatus(channelReference, streamingChannel, cachedTrackedChannelStatus);
            } else if (cachedTrackedChannelStatus == null) {
                // Create initial Channel Status when cached status not found
                channelStatus = getInitialChannelStatus(streamingChannel);
                requestedChannelReferences.add(channelReference);
            } else {
                channelStatus = cachedTrackedChannelStatus.channelStatus;
            }
        } else {
            channelStatus = cachedTrackedChannelStatus.channelStatus;
        }

        return channelStatus;
    }

    @Override
    public boolean isChannelStatusRefreshRequired(final StreamingChannel streamingChannel) {
        Objects.requireNonNull(streamingChannel, "Streaming Channel required");

        final ChannelReference channelReference = new ChannelReference(
                streamingChannel.getDatabase(),
                streamingChannel.getSchema(),
                streamingChannel.getPipe(),
                streamingChannel.getChannel()
        );

        final TrackedChannelStatus cachedTrackedChannelStatus = getCachedTrackedChannelStatus(channelReference);
        return isChannelRefreshRequired(cachedTrackedChannelStatus) && isRefreshRequired();
    }

    private TrackedChannelStatus getCachedTrackedChannelStatus(final ChannelReference channelReference) {
        final Lock readLock = lock.readLock();
        readLock.lock();
        try {
            return channelReferences.get(channelReference);
        } finally {
            readLock.unlock();
        }
    }

    private ChannelStatus getCurrentChannelStatus(
            final ChannelReference channelReference,
            final StreamingChannel streamingChannel,
            final TrackedChannelStatus trackedChannelStatus
    ) {
        final ChannelStatus channelStatus;

        final Lock writeLock = lock.writeLock();
        if (writeLock.tryLock()) {
            try {
                removeExpiredChannelReferences();
                channelStatus = getChannelStatus(channelReference);

                final Instant now = clock.instant();
                final Instant nextExpiration = now.plus(EXPIRATION_PERIOD);
                expiration.set(nextExpiration.toEpochMilli());
            } finally {
                writeLock.unlock();
            }
        } else if (trackedChannelStatus == null) {
            // Create initial Channel Status in place of cached status
            channelStatus = getInitialChannelStatus(streamingChannel);
            requestedChannelReferences.add(channelReference);
        } else {
            channelStatus = trackedChannelStatus.channelStatus;
        }
        return channelStatus;
    }

    private boolean isChannelRefreshRequired(final TrackedChannelStatus trackedChannelStatus) {
        final boolean refreshRequired;

        if (trackedChannelStatus == null) {
            refreshRequired = true;
        } else {
            final Instant expiration = trackedChannelStatus.expiration;
            final Instant now = clock.instant();
            refreshRequired = now.isAfter(expiration);
        }

        return refreshRequired;
    }

    private boolean isRefreshRequired() {
        final long currentExpiration = expiration.get();
        final long now = clock.instant().toEpochMilli();
        return now >= currentExpiration;
    }

    private ChannelStatus getChannelStatus(final ChannelReference channelReference) {
        final String database = channelReference.database;
        final String schema = channelReference.schema;
        final String pipe = channelReference.pipe;

        // Build Channel Names for Bulk Channel Status from current and cached references
        final Map<String, ChannelReference> selectedChannelReferences = new HashMap<>();
        selectedChannelReferences.put(channelReference.channel, channelReference);
        channelReferences.remove(channelReference);

        channelReferences.keySet().stream()
                .filter(reference -> database.equals(reference.database))
                .filter(reference -> schema.equals(reference.schema))
                .filter(reference -> pipe.equals(reference.pipe))
                .forEach(reference ->
                    selectedChannelReferences.put(reference.channel, reference)
                );

        requestedChannelReferences.stream()
                .filter(reference -> database.equals(reference.database))
                .filter(reference -> schema.equals(reference.schema))
                .filter(reference -> pipe.equals(reference.pipe))
                .forEach(reference ->
                        selectedChannelReferences.put(reference.channel, reference)
                );

        final List<String> selectedChannelNames = new ArrayList<>(selectedChannelReferences.keySet());
        final BulkChannelNames bulkChannelNames = new BulkChannelNames(selectedChannelNames);

        final UUID requestId = UUID.randomUUID();
        final BulkChannelStatus bulkChannelStatus = streamingChannelClient.getBulkChannelStatus(database, schema, pipe, bulkChannelNames, requestId);
        logger.info("Requested Bulk Channel Status for Pipe [{}.{}.{}] Channels [{}] Request ID [{}]", database, schema, pipe, selectedChannelNames.size(), requestId);

        setChannelReferences(bulkChannelStatus, selectedChannelReferences);
        final TrackedChannelStatus trackedChannelStatus = channelReferences.get(channelReference);

        final ChannelStatus channelStatus;
        if (trackedChannelStatus == null) {
            logger.info("Channel [{}] not found in Channel Status for Pipe [{}.{}.{}] Request ID [{}]", channelReference.channel, database, schema, pipe, requestId);
            channelStatus = getNotFoundChannelStatus(channelReference);
        } else {
            channelStatus = trackedChannelStatus.channelStatus;
        }

        return channelStatus;
    }

    private void setChannelReferences(final BulkChannelStatus bulkChannelStatus, final Map<String, ChannelReference> selectedChannelReferences) {
        final Instant now = clock.instant();
        final Instant expiration = now.plus(EXPIRATION_PERIOD);
        for (final Map.Entry<String, ChannelStatus> channelStatusEntry : bulkChannelStatus.channelStatuses().entrySet()) {
            final String channel = channelStatusEntry.getKey();
            final ChannelReference selectedChannelReference = selectedChannelReferences.get(channel);

            final Instant trackingExpiration;
            final TrackedChannelStatus previousTrackedChannelStatus = channelReferences.get(selectedChannelReference);
            if (previousTrackedChannelStatus == null) {
                // Set Tracking Expiration for current requested Channel
                trackingExpiration = now.plus(TRACKING_PERIOD);
            } else {
                // Retain previous Tracking Expiration for other Channels
                trackingExpiration = previousTrackedChannelStatus.trackingExpiration;
            }

            final ChannelStatus channelStatus = channelStatusEntry.getValue();
            final TrackedChannelStatus trackedChannelStatus = new TrackedChannelStatus(channelStatus, expiration, trackingExpiration);
            channelReferences.put(selectedChannelReference, trackedChannelStatus);

            // Remove Selected Channel Reference after adding Tracked Channel Status
            requestedChannelReferences.remove(selectedChannelReference);
        }
    }

    private void removeExpiredChannelReferences() {
        final Instant now = clock.instant();

        final List<ChannelReference> expiredChannelReferences = channelReferences.entrySet()
                .stream()
                .filter(entry -> {
                    final Instant trackingExpiration = entry.getValue().trackingExpiration;
                    return now.isAfter(trackingExpiration);
                })
                .map(Map.Entry::getKey)
                .toList();

        for (final ChannelReference channelReference : expiredChannelReferences) {
            channelReferences.remove(channelReference);
            logger.debug("Removed expired Channel Status [{}]", channelReference);
        }
    }

    private ChannelStatus getInitialChannelStatus(final StreamingChannel streamingChannel) {
        final long created = clock.instant().toEpochMilli();
        return new ChannelStatus(
                ChannelStatusCode.SUCCESS.getStatus(),
                streamingChannel.getCommittedOffsetToken(),
                created,
                streamingChannel.getDatabase(),
                streamingChannel.getSchema(),
                streamingChannel.getPipe(),
                streamingChannel.getChannel(),
                EMPTY_COUNT,
                EMPTY_COUNT,
                streamingChannel.getRowsErrorCount(),
                ChannelStatusCode.SUCCESS.ordinal(),
                null,
                0,
                null,
                null,
                0
        );
    }

    private ChannelStatus getNotFoundChannelStatus(final ChannelReference channelReference) {
        final long created = clock.instant().toEpochMilli();
        return new ChannelStatus(
                ChannelStatusCode.ERR_CHANNEL_NO_LONGER_EXISTS.getStatus(),
                null,
                created,
                channelReference.database,
                channelReference.schema,
                channelReference.pipe,
                channelReference.channel,
                EMPTY_COUNT,
                EMPTY_COUNT,
                EMPTY_COUNT,
                CHANNEL_NOT_FOUND_STATUS_CODE,
                null,
                0,
                null,
                CHANNEL_NOT_FOUND_MESSAGE,
                0
        );
    }

    record ChannelReference(
            String database,
            String schema,
            String pipe,
            String channel
    ) {

    }

    record TrackedChannelStatus(
            ChannelStatus channelStatus,
            Instant expiration,
            Instant trackingExpiration
    ) {

    }
}
