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

import net.snowflake.openflow.components.snowpipe.streaming.HttpClientException;
import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.ChannelStatusProvider;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.OffsetTokenCommittedCommand;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannelStatus;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ChannelType;
import net.snowflake.openflow.processors.snowpipe.streaming.property.OffsetTrackingResolution;
import net.snowflake.openflow.processors.snowpipe.streaming.property.TransferStrategy;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import tools.jackson.core.JsonPointer;

import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.ACCOUNT;
import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.AUTHENTICATION_STRATEGY;
import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.CONNECTION_STRATEGY;
import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.PRIVATE_KEY_SERVICE;
import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.ROLE;
import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.USER;
import static net.snowflake.openflow.processors.snowpipe.streaming.StandardStreamingClientProvider.WEB_CLIENT_SERVICE_PROVIDER;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.CHANNEL_GROUP;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.CHANNEL_TYPE;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.DATABASE;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.DESTINATION_TYPE;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.PIPE;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.SCHEMA;
import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.TABLE;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@TriggerWhenEmpty
@Tags({"Snowflake", "Snowpipe Streaming", "NDJSON"})
@CapabilityDescription("""
    Publish Records formatted as Newline Delimited JSON to Snowflake Database Pipes using Snowpipe Streaming Version 2.
    """)
@WritesAttribute(
        attribute = ChannelStatusAttributes.CHANNEL_STATUS_ROW_ERRORS_ADDED,
        description = "Number of row errors added to the Channel for the transfer. Always written, set to 0 when no row errors are reported."
)
public class PublishSnowpipeStreaming extends AbstractSessionFactoryProcessor implements VerifiableProcessor {

    static final PropertyDescriptor TRANSFER_STRATEGY = new PropertyDescriptor.Builder()
            .name("Transfer Strategy")
            .description("Strategy for transferring records to Snowpipe Streaming")
            .required(true)
            .defaultValue(TransferStrategy.MANAGED)
            .allowableValues(TransferStrategy.class)
            .build();

    static final PropertyDescriptor FILE_FRAGMENT_SIZE = new PropertyDescriptor.Builder()
            .name("File Fragment Size")
            .description("""
            Maximum size in bytes for each File Fragment sent to object storage for Snowpipe Streaming ingestion.
            Must be between 1 KB and 256 MB
            """
            )
            .required(true)
            .defaultValue("64 MB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1024, 268435456))
            .dependsOn(TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS)
            .build();

    static final PropertyDescriptor FILE_FRAGMENT_COUNT = new PropertyDescriptor.Builder()
            .name("File Fragment Count")
            .description("""
            Maximum number of File Fragments sent to object storage for Snowpipe Streaming ingestion from input FlowFiles.
            Must be between 1 and 100.
            """
            )
            .required(true)
            .defaultValue("4")
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .dependsOn(TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS)
            .build();

    static final PropertyDescriptor OFFSET_TRACKING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Offset Tracking Timeout")
            .description("Maximum duration to wait for channel status to confirm committed offset tokens before routing to failure")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("90 s")
            .dependsOn(CHANNEL_TYPE, ChannelType.STANDARD)
            .build();

    static final PropertyDescriptor OFFSET_TRACKING_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Offset Tracking Resolution")
            .description("Resolution level for evaluating committed Offset Tokens against input FlowFiles and Records")
            .required(true)
            .allowableValues(OffsetTrackingResolution.class)
            .defaultValue(OffsetTrackingResolution.DISABLED)
            .dependsOn(CHANNEL_TYPE, ChannelType.STANDARD)
            .build();

    static final PropertyDescriptor OFFSET_TOKEN_START_EXPRESSION = new PropertyDescriptor.Builder()
            .name("Offset Token Start Expression")
            .description("Expression Language definition to produce the lowest offset token for a FlowFile as a monotonically increasing number")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(OFFSET_TRACKING_RESOLUTION, OffsetTrackingResolution.RECORD)
            .dependsOn(CHANNEL_TYPE, ChannelType.STANDARD)
            .build();

    static final PropertyDescriptor OFFSET_TOKEN_END_EXPRESSION = new PropertyDescriptor.Builder()
            .name("Offset Token End Expression")
            .description("Expression Language definition to produce the highest offset token for a FlowFile as a monotonically increasing number")
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(OFFSET_TRACKING_RESOLUTION, OffsetTrackingResolution.RECORD, OffsetTrackingResolution.FLOW_FILE)
            .dependsOn(CHANNEL_TYPE, ChannelType.STANDARD)
            .build();

    static final PropertyDescriptor OFFSET_TOKEN_RECORD_POINTER = new PropertyDescriptor.Builder()
            .name("Offset Token Record Pointer")
            .description("JSON Pointer to offset token in each record required when the last committed offset token is between start and end boundaries")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .dependsOn(OFFSET_TRACKING_RESOLUTION, OffsetTrackingResolution.RECORD)
            .dependsOn(CHANNEL_TYPE, ChannelType.STANDARD)
            .build();

    static final PropertyDescriptor CHANNEL_INSERT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Channel Insert Timeout")
            .description("Maximum duration to retry inserting records before failing with an upper bound of 5 minutes")
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, 300, TimeUnit.SECONDS))
            .defaultValue("30 s")
            .required(true)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles successfully uploaded to Snowflake")
            .build();

    static final Relationship EMPTY = new Relationship.Builder()
            .name("empty")
            .description("FlowFiles with empty content not sent to Snowflake")
            .autoTerminateDefault(true)
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to upload to Snowflake")
            .build();

    static final Relationship INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that Snowflake identified as containing one or more invalid rows resulting in partial transmission")
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AUTHENTICATION_STRATEGY,
            CONNECTION_STRATEGY,
            ACCOUNT,
            USER,
            ROLE,
            PRIVATE_KEY_SERVICE,
            DESTINATION_TYPE,
            DATABASE,
            SCHEMA,
            PIPE,
            TABLE,
            WEB_CLIENT_SERVICE_PROVIDER,
            TRANSFER_STRATEGY,
            CHANNEL_TYPE,
            FILE_FRAGMENT_SIZE,
            FILE_FRAGMENT_COUNT,
            OFFSET_TRACKING_TIMEOUT,
            OFFSET_TRACKING_RESOLUTION,
            OFFSET_TOKEN_START_EXPRESSION,
            OFFSET_TOKEN_END_EXPRESSION,
            OFFSET_TOKEN_RECORD_POINTER,
            CHANNEL_GROUP,
            CHANNEL_INSERT_TIMEOUT
    );

    static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS,
            EMPTY,
            FAILURE,
            INVALID
    );

    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final Set<Integer> INFORMATIONAL_STATUS_CODES = Set.of(
            HTTP_TOO_MANY_REQUESTS,
            HttpURLConnection.HTTP_UNAVAILABLE
    );

    protected StreamingClientProvider streamingClientProvider = new StandardStreamingClientProvider();

    private final StreamingDestinationManager streamingDestinationManager = new StreamingDestinationManager();

    private ChannelType channelType = ChannelType.STANDARD;

    private SnowpipeStreamingService snowpipeStreamingService;

    private Duration offsetTrackingTimeout;

    private long maximumBatchSize;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration propertyConfiguration) {
        StreamingDestinationProvider.migrateProperties(propertyConfiguration);
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog componentLog, final Map<String, String> attributes) {
        return streamingClientProvider.verify(context, componentLog, attributes);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        final WebClientService webClientService = webClientServiceProvider.getWebClientService();

        final URI streamingUri = streamingClientProvider.getStreamingUri(context);
        final StreamingChannelClient streamingChannelClient = streamingClientProvider.getStreamingChannelClient(context, streamingUri);

        final TransferStrategy transferStrategy = context.getProperty(TRANSFER_STRATEGY).asAllowableValue(TransferStrategy.class);
        final int maximumBufferSize;
        if (TransferStrategy.FILE_FRAGMENTS == transferStrategy) {
            final Double fileFragmentSize = context.getProperty(FILE_FRAGMENT_SIZE).asDataSize(DataUnit.B);
            final int fileFragmentCount = context.getProperty(FILE_FRAGMENT_COUNT).asInteger();
            maximumBatchSize = fileFragmentSize.longValue() * fileFragmentCount;
            maximumBufferSize = fileFragmentSize.intValue();
        } else {
            maximumBatchSize = transferStrategy.getBatchSize();
            maximumBufferSize = transferStrategy.getBufferSize();
        }

        channelType = context.getProperty(CHANNEL_TYPE).asAllowableValue(ChannelType.class);
        if (ChannelType.STANDARD == channelType) {
            offsetTrackingTimeout = context.getProperty(OFFSET_TRACKING_TIMEOUT).asDuration();
        } else {
            offsetTrackingTimeout = Duration.ZERO;
        }

        final OffsetTrackingResolution offsetTrackingResolution;

        if (ChannelType.STANDARD == channelType) {
            offsetTrackingResolution = context.getProperty(OFFSET_TRACKING_RESOLUTION).asAllowableValue(OffsetTrackingResolution.class);
        } else {
            offsetTrackingResolution = OffsetTrackingResolution.DISABLED;
        }

        final JsonPointer offsetTokenRecordPointer;
        if (OffsetTrackingResolution.RECORD == offsetTrackingResolution) {
            final String pointerProperty = context.getProperty(OFFSET_TOKEN_RECORD_POINTER).getValue();
            offsetTokenRecordPointer = JsonPointer.compile(pointerProperty);
        } else {
            offsetTokenRecordPointer = null;
        }

        final Duration channelInsertTimeout = context.getProperty(CHANNEL_INSERT_TIMEOUT).asDuration();

        snowpipeStreamingService = new StandardSnowpipeStreamingService(
                getLogger(),
                streamingUri,
                webClientService,
                streamingChannelClient,
                transferStrategy,
                channelType,
                offsetTrackingResolution,
                channelInsertTimeout,
                offsetTokenRecordPointer,
                maximumBufferSize
        );
    }

    @OnStopped
    public void onStopped() {
        for (final Map.Entry<StreamingDestination, PendingBatch> entry : streamingDestinationManager.getPendingDestinations()) {
            final StreamingDestination destination = entry.getKey();
            final PendingBatch pendingBatch = entry.getValue();
            try {
                final ProcessSession batchSession = pendingBatch.getSession();
                batchSession.rollback();
            } catch (final Exception e) {
                getLogger().warn("Failed to rollback pending batch session for {}", destination, e);
            }
            snowpipeStreamingService.returnChannel(pendingBatch.getStreamingChannel(), pendingBatch.getStreamingDestination());
            streamingDestinationManager.releaseDestination(destination);
        }

        snowpipeStreamingService.close();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        if (ChannelType.STANDARD == channelType) {
            processStandardChannel(context, sessionFactory);
        } else {
            processElasticChannel(context, sessionFactory);
        }
    }

    private void processElasticChannel(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        // Streaming Destination Manager is not necessary for Elastic Channels when filtering input FlowFiles
        final StreamingDestinationFlowFileFilter filter = new StreamingDestinationFlowFileFilter(context, maximumBatchSize);
        final ProcessSession session = sessionFactory.createSession();

        final List<FlowFile> flowFiles = session.get(filter);
        if (flowFiles.isEmpty()) {
            context.yield();
        } else {
            final FlowFile first = flowFiles.getFirst();
            if (first.getSize() == 0) {
                session.transfer(first, EMPTY);
            } else {
                final StreamingDestination streamingDestination = filter.getStreamingDestination();
                transferElasticChannel(context, session, flowFiles, streamingDestination);
            }
        }

        session.commitAsync();
    }

    private void transferElasticChannel(
            final ProcessContext context,
            final ProcessSession session,
            final List<FlowFile> flowFiles,
            final StreamingDestination streamingDestination
    ) {
        // Construct Streaming Channel for selected Streaming Destination instead of using StreamingChannelFactory
        final StreamingChannel streamingChannel = new StreamingChannel(
                streamingDestination.database(),
                streamingDestination.schema(),
                streamingDestination.pipe(),
                streamingDestination.channelGroup()
        );

        final long started = System.nanoTime();
        try {
            snowpipeStreamingService.transferFlowFiles(context, session, flowFiles, streamingChannel);
            final long processingElapsed = getDuration(started);
            session.adjustCounter(StandardSnowpipeStreamingService.SessionCounter.PROCESSING_DURATION.counter, processingElapsed, true);

            session.transfer(flowFiles, SUCCESS);
        } catch (final Exception e) {
            getLogger().error("Failed to process FlowFiles [{}]", flowFiles.size(), e);
            session.transfer(flowFiles, FAILURE);
        }
    }

    private void processStandardChannel(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        processPendingBatches();

        final ProcessSession session = sessionFactory.createSession();
        final BatchedDestinationFlowFilter filter = new BatchedDestinationFlowFilter(context, maximumBatchSize, streamingDestinationManager);
        final List<FlowFile> flowFiles = session.get(filter);
        if (flowFiles.isEmpty()) {
            session.commitAsync();

            if (isYieldRequired()) {
                context.yield();
            }

            return;
        }

        final StreamingDestination streamingDestination = filter.getStreamingDestination();

        final FlowFile first = flowFiles.getFirst();
        if (first.getSize() == 0) {
            session.transfer(first, EMPTY);
            // Commit Session and release Destination without having acquired Channel
            commitAsyncReleaseDestination(session, streamingDestination);
            return;
        }

        final Optional<StreamingChannel> streamingChannelFound = snowpipeStreamingService.borrowChannel(streamingDestination);
        if (streamingChannelFound.isEmpty()) {
            context.yield();
            session.transfer(flowFiles, FAILURE);
            commitAsyncReleaseDestination(session, streamingDestination);
            getLogger().warn("Failed to open Channel for [{}] FlowFiles [{}]", streamingDestination, flowFiles.size());
        } else {
            final StreamingChannel streamingChannel = streamingChannelFound.get();
            try {
                final long started = System.nanoTime();
                snowpipeStreamingService.transferFlowFiles(context, session, flowFiles, streamingChannel);
                final long processingElapsed = getDuration(started);
                session.adjustCounter(StandardSnowpipeStreamingService.SessionCounter.PROCESSING_DURATION.counter, processingElapsed, true);

                final OffsetTokenCommittedCommand command = new OffsetTokenCommittedCommand(snowpipeStreamingService.getChannelStatusProvider(), streamingChannel);
                final PendingBatch pendingBatch = new PendingBatch(session, flowFiles, streamingChannel, streamingDestination, Instant.now(), command);
                streamingDestinationManager.registerPendingBatch(streamingDestination, pendingBatch);
            } catch (final Exception e) {
                getLogger().error("Failed to process FlowFiles [{}]", flowFiles.size(), e);
                session.transfer(flowFiles, FAILURE);
                // Commit Session and release Destination after invalidating Channel
                commitAsyncInvalidateChannel(session, streamingChannel, streamingDestination);
            }
        }
    }

    private boolean isYieldRequired() {
        final ChannelStatusProvider channelStatusProvider = snowpipeStreamingService.getChannelStatusProvider();

        for (final Map.Entry<StreamingDestination, PendingBatch> entry : streamingDestinationManager.getPendingDestinations()) {
            final PendingBatch pendingBatch = entry.getValue();
            if (channelStatusProvider.isChannelStatusRefreshRequired(pendingBatch.getStreamingChannel())) {
                return false;
            }
        }

        return true;
    }

    private void processPendingBatches() {
        for (final Map.Entry<StreamingDestination, PendingBatch> entry : streamingDestinationManager.getPendingDestinations()) {
            final PendingBatch pendingBatch = entry.getValue();

            if (!pendingBatch.tryClaim()) {
                continue;
            }

            if (!processPendingBatch(pendingBatch)) {
                pendingBatch.releaseClaim();
            }
        }
    }

    private boolean processPendingBatch(final PendingBatch pendingBatch) {
        final OffsetTokenCommittedCommand command = pendingBatch.getOffsetTokenCommittedCommand();
        final StreamingChannel streamingChannel = pendingBatch.getStreamingChannel();
        final StreamingDestination streamingDestination = pendingBatch.getStreamingDestination();
        final ProcessSession batchSession = pendingBatch.getSession();
        final List<FlowFile> flowFiles = pendingBatch.getFlowFiles();
        final String offsetToken = streamingChannel.getOffsetToken();

        command.run();

        final boolean batchCompleted;
        final StreamingChannelStatus streamingChannelStatus = command.getStreamingChannelStatus();
        final Instant expiration = pendingBatch.getCreated().plus(offsetTrackingTimeout);
        final String committedOffsetToken = streamingChannel.getCommittedOffsetToken();

        if (StreamingChannelStatus.FAILURE == streamingChannelStatus) {
            final String lastErrorMessage = command.getLastErrorMessage();
            final long pollingDuration = processPollingDuration(batchSession, pendingBatch);
            getLogger().error("{} Polling Failed after {} ms for Offset Token [{}] {}",
                    streamingChannel, pollingDuration, offsetToken, lastErrorMessage
            );

            batchSession.transfer(ChannelStatusAttributes.apply(batchSession, flowFiles, 0L), FAILURE);

            // Commit Session and release Destination after invalidating Channel
            commitAsyncInvalidateChannel(batchSession, streamingChannel, streamingDestination);
            batchCompleted = true;
        } else if (command.isOffsetTokenCommitted()) {
            final long pollingDuration = processPollingDuration(batchSession, pendingBatch);
            final long lastErrorCount = command.getLastErrorCount();
            if (StreamingChannelStatus.INVALID == streamingChannelStatus) {
                final String lastErrorMessage = command.getLastErrorMessage();
                getLogger().warn("{} Polling Completed after {} ms for Offset Token [{}] with Invalid Rows [{}] Last Error [{}]",
                        streamingChannel, pollingDuration, offsetToken, lastErrorCount, lastErrorMessage
                );

                batchSession.transfer(ChannelStatusAttributes.apply(batchSession, flowFiles, lastErrorCount), INVALID);
            } else {
                getLogger().info("{} Polling Completed after {} ms for Offset Token [{}] based on Channel Committed Offset Token [{}]",
                        streamingChannel, pollingDuration, offsetToken, committedOffsetToken
                );

                batchSession.transfer(ChannelStatusAttributes.apply(batchSession, flowFiles, 0L), SUCCESS);
            }

            // Commit Session and release Destination after returning Channel
            commitAsyncReturnChannel(batchSession, streamingChannel, streamingDestination);
            batchCompleted = true;
        } else if (Instant.now().isAfter(expiration)) {
            final long pollingDuration = processPollingDuration(batchSession, pendingBatch);
            getLogger().error("{} Polling Failed after {} ms for Offset Token [{}] based on Channel Committed Offset Token [{}]",
                    streamingChannel, pollingDuration, offsetToken, committedOffsetToken, command.getLastException()
            );

            batchSession.transfer(ChannelStatusAttributes.apply(batchSession, flowFiles, 0L), FAILURE);
            commitAsyncInvalidateChannel(batchSession, streamingChannel, streamingDestination);
            batchCompleted = true;
        } else {
            processCommandPendingStatus(streamingChannel, command, expiration);
            batchCompleted = false;
        }

        return batchCompleted;
    }

    private void processCommandPendingStatus(
            final StreamingChannel streamingChannel,
            final OffsetTokenCommittedCommand command,
            final Instant expiration
    ) {
        final int runCount = command.getRunCount();
        final Exception lastException = command.getLastException();
        if (lastException == null) {
            getLogger().debug("{} Pending Channel Status on attempt [{}]", streamingChannel, runCount);
        } else {
            final LogLevel logLevel;

            if (lastException instanceof HttpResponseException httpResponseException) {
                final int statusCode = httpResponseException.getStatusCode();
                if (INFORMATIONAL_STATUS_CODES.contains(statusCode)) {
                    logLevel = LogLevel.INFO;
                } else {
                    logLevel = LogLevel.WARN;
                }
            } else if (lastException instanceof HttpClientException) {
                // Handle HttpClientException for communication timeouts as informational
                logLevel = LogLevel.INFO;
            } else {
                logLevel = LogLevel.WARN;
            }

            final long secondsRemaining = Duration.between(Instant.now(), expiration).toSeconds();
            getLogger().log(logLevel, "{} unable to get Channel Status on attempt [{}] retrying for {} s", streamingChannel, runCount, secondsRemaining, lastException);
        }
    }

    private long processPollingDuration(final ProcessSession session, final PendingBatch pendingBatch) {
        final Instant created = pendingBatch.getCreated();
        final long pollingDuration = Duration.between(created, Instant.now()).toMillis();

        session.adjustCounter(StandardSnowpipeStreamingService.SessionCounter.OFFSET_TOKEN_POLLING_DURATION.counter, pollingDuration, true);
        return pollingDuration;
    }

    private void commitAsyncReleaseDestination(
            final ProcessSession session,
            final StreamingDestination streamingDestination
    ) {
        session.commitAsync(
                () -> streamingDestinationManager.releaseDestination(streamingDestination),
                exception -> streamingDestinationManager.releaseDestination(streamingDestination)
        );
    }

    private void commitAsyncReturnChannel(
            final ProcessSession session,
            final StreamingChannel streamingChannel,
            final StreamingDestination streamingDestination
    ) {
        session.commitAsync(
                () -> returnChannel(streamingChannel, streamingDestination),
                exception -> returnChannel(streamingChannel, streamingDestination)
        );
    }

    private void returnChannel(final StreamingChannel streamingChannel, final StreamingDestination streamingDestination) {
        snowpipeStreamingService.returnChannel(streamingChannel, streamingDestination);
        streamingDestinationManager.releaseDestination(streamingDestination);
    }

    private void commitAsyncInvalidateChannel(
            final ProcessSession session,
            final StreamingChannel streamingChannel,
            final StreamingDestination streamingDestination
    ) {
        session.commitAsync(
                () -> invalidateChannel(streamingChannel, streamingDestination),
                exception -> invalidateChannel(streamingChannel, streamingDestination)
        );
    }

    private void invalidateChannel(final StreamingChannel streamingChannel, final StreamingDestination streamingDestination) {
        snowpipeStreamingService.invalidateChannel(streamingChannel, streamingDestination);
        streamingDestinationManager.releaseDestination(streamingDestination);
    }

    private long getDuration(final long started) {
        final long elapsed = System.nanoTime() - started;
        final Duration duration = Duration.ofNanos(elapsed);
        return duration.toMillis();
    }
}
