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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.InsertStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.RowsetStageLocation;
import net.snowflake.openflow.components.snowpipe.streaming.security.StandardBufferEncryptor;
import net.snowflake.openflow.components.snowpipe.streaming.transfer.ObjectTransferClient;
import net.snowflake.openflow.components.snowpipe.streaming.transfer.ObjectTransferLocation;
import net.snowflake.openflow.components.snowpipe.streaming.transfer.StandardObjectTransferClient;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.ChannelStatusProvider;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.PipeChannelStatusProvider;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannel;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannelFactory;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ChannelType;
import net.snowflake.openflow.processors.snowpipe.streaming.property.OffsetTrackingResolution;
import net.snowflake.openflow.processors.snowpipe.streaming.property.TransferStrategy;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.web.client.api.WebClientService;
import tools.jackson.core.JsonPointer;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static net.snowflake.openflow.processors.snowpipe.streaming.PublishSnowpipeStreaming.OFFSET_TOKEN_END_EXPRESSION;
import static net.snowflake.openflow.processors.snowpipe.streaming.PublishSnowpipeStreaming.OFFSET_TOKEN_START_EXPRESSION;

class StandardSnowpipeStreamingService implements SnowpipeStreamingService {

    private static final ObjectMapper MAPPER = JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();

    private static final String TRANSIT_URI_FORMAT = "https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s";

    private static final byte LINE_FEED = 10;

    private static final long FILE_FRAGMENT_RETRY_INTERVAL = 250;

    private static final int INPUT_BUFFER_READ_SIZE = 8192;

    private static final int END_OF_STREAM = -1;

    private static final int COMPRESSED_BUFFER_RATIO = 4;

    private static final int ROWS_MAXIMUM_CONTENT_LENGTH = 4194304;

    /** ZStandard Magic Number defined as 0xFD2FB528 according to RFC 8878 Section 3.1.1 */
    private static final byte[] ZSTANDARD_MAGIC_NUMBER = {40, -75, 47, -3};

    private final ComponentLog logger;

    private final URI streamingUri;

    private final StreamingChannelClient streamingChannelClient;

    private final ObjectTransferClient objectTransferClient;

    private final KeyedObjectPool<StreamingDestination, StreamingChannel> streamingChannelPool;

    private final ChannelStatusProvider channelStatusProvider;

    private final TransferStrategy transferStrategy;

    private final ChannelType channelType;

    private final OffsetTrackingResolution offsetTrackingResolution;

    private final Duration channelInsertTimeout;

    private final JsonPointer offsetTokenRecordPointer;

    private final int maximumBufferSize;

    private final int compressedBufferSize;

    private final ConcurrentMap<StreamingDestination, PipeInfo> pipeInfos = new ConcurrentHashMap<>();

    StandardSnowpipeStreamingService(
            final ComponentLog logger,
            final URI streamingUri,
            final WebClientService webClientService,
            final StreamingChannelClient streamingChannelClient,
            final TransferStrategy transferStrategy,
            final ChannelType channelType,
            final OffsetTrackingResolution offsetTrackingResolution,
            final Duration channelInsertTimeout,
            final JsonPointer offsetTokenRecordPointer,
            final int maximumBufferSize
    ) {
        this.logger = logger;
        this.streamingUri = streamingUri;
        this.streamingChannelClient = streamingChannelClient;
        this.transferStrategy = transferStrategy;
        this.channelType = channelType;
        this.offsetTrackingResolution = offsetTrackingResolution;
        this.channelInsertTimeout = channelInsertTimeout;
        this.offsetTokenRecordPointer = offsetTokenRecordPointer;
        this.maximumBufferSize = maximumBufferSize;
        this.compressedBufferSize = maximumBufferSize / COMPRESSED_BUFFER_RATIO;

        this.objectTransferClient = new StandardObjectTransferClient(webClientService, new StandardBufferEncryptor());

        final GenericKeyedObjectPoolConfig<StreamingChannel> poolConfig = getPoolConfig();
        final StreamingChannelFactory streamingChannelFactory = new StreamingChannelFactory(logger, streamingChannelClient);
        this.streamingChannelPool = new GenericKeyedObjectPool<>(streamingChannelFactory, poolConfig);

        this.channelStatusProvider = new PipeChannelStatusProvider(logger, streamingChannelClient, Clock.systemDefaultZone());
    }

    @Override
    public Optional<StreamingChannel> borrowChannel(final StreamingDestination streamingDestination) {
        try {
            final StreamingChannel streamingChannel = streamingChannelPool.borrowObject(streamingDestination);
            return Optional.of(streamingChannel);
        } catch (final NoSuchElementException e) {
            logger.debug("Channel not available for {}", streamingDestination);
            return Optional.empty();
        } catch (final IllegalStateException e) {
            logger.debug("Pool not available for {}", streamingDestination);
            return Optional.empty();
        } catch (final Exception e) {
            logger.warn("Failed to get Channel for {}", streamingDestination, e);
            return Optional.empty();
        }
    }

    @Override
    public void returnChannel(final StreamingChannel streamingChannel, final StreamingDestination streamingDestination) {
        try {
            streamingChannelPool.returnObject(streamingDestination, streamingChannel);
        } catch (final Exception e) {
            logger.warn("Failed to return {}", streamingChannel, e);
        }
    }

    @Override
    public void invalidateChannel(final StreamingChannel streamingChannel, final StreamingDestination streamingDestination) {
        try {
            streamingChannelPool.invalidateObject(streamingDestination, streamingChannel);
        } catch (final Exception e) {
            logger.warn("Failed to invalidate {}", streamingChannel, e);
        }
    }

    @Override
    public void transferFlowFiles(
            final ProcessContext context,
            final ProcessSession session,
            final List<FlowFile> flowFiles,
            final StreamingChannel streamingChannel
    ) {
        final AtomicLong inputBytes = new AtomicLong(0);

        final AtomicLong bufferedRows = new AtomicLong();
        final List<FlowFile> bufferedFlowFiles = new ArrayList<>();
        final String transitUri = getTransitUri(streamingChannel);
        final ProvenanceReporter provenanceReporter = session.getProvenanceReporter();
        final AtomicLong bufferingStarted = new AtomicLong(System.nanoTime());

        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(compressedBufferSize);
        OutputStream compressedBuffer = getCompressedOutputStream(buffer);

        String currentFlowFileOffsetToken = null;
        for (final FlowFile flowFile : flowFiles) {
            final FlowFileOffsetStatus flowFileOffsetStatus = getFlowFileOffsetStatus(context, flowFile);
            final FlowFileTrackingStatus flowFileTrackingStatus = getFlowFileTrackingStatus(flowFileOffsetStatus, streamingChannel);

            if (FlowFileTrackingStatus.COMMITTED == flowFileTrackingStatus) {
                session.adjustCounter(SessionCounter.FLOW_FILES_SKIPPED.counter, 1, false);
                logger.info("Skipped {} for {} with End Offset Token [{}] Committed Offset Token [{}]",
                    flowFile, streamingChannel, flowFileOffsetStatus.endOffsetToken(), streamingChannel.getCommittedOffsetToken()
                );
                continue;
            }

            transferFlowFile(session, flowFile, flowFileTrackingStatus, bufferedRows, inputBytes, compressedBuffer, streamingChannel);
            bufferedFlowFiles.add(flowFile);

            if (ChannelType.STANDARD == channelType) {
                // Offset Token required for Standard Channels to support polling for committed status
                currentFlowFileOffsetToken = flowFileOffsetStatus.endOffsetToken.toString();
            }

            if (inputBytes.get() >= maximumBufferSize) {
                closeOutputStream(compressedBuffer);
                insertBuffer(session, inputBytes, buffer, bufferedRows, currentFlowFileOffsetToken, streamingChannel);
                reportBufferedFlowFiles(provenanceReporter, bufferedFlowFiles, transitUri, bufferingStarted);
                compressedBuffer = getCompressedOutputStream(buffer);
            }
        }

        if (inputBytes.get() > 0) {
            closeOutputStream(compressedBuffer);
            insertBuffer(session, inputBytes, buffer, bufferedRows, currentFlowFileOffsetToken, streamingChannel);
            reportBufferedFlowFiles(provenanceReporter, bufferedFlowFiles, transitUri, bufferingStarted);
        }
    }

    @Override
    public ChannelStatusProvider getChannelStatusProvider() {
        return channelStatusProvider;
    }

    @Override
    public String getTransitUri(final StreamingChannel streamingChannel) {
        final String database = URLEncoder.encode(streamingChannel.getDatabase(), StandardCharsets.UTF_8);
        final String schema = URLEncoder.encode(streamingChannel.getSchema(), StandardCharsets.UTF_8);
        final String pipe = URLEncoder.encode(streamingChannel.getPipe(), StandardCharsets.UTF_8);
        final String channel = URLEncoder.encode(streamingChannel.getChannel(), StandardCharsets.UTF_8);
        return TRANSIT_URI_FORMAT.formatted(streamingUri.getHost(), database, schema, pipe, channel);
    }

    @Override
    public void close() {
        streamingChannelPool.close();
        pipeInfos.clear();
    }

    private void transferFlowFile(
            final ProcessSession session,
            final FlowFile flowFile,
            final FlowFileTrackingStatus flowFileTrackingStatus,
            final AtomicLong bufferedRows,
            final AtomicLong inputBytes,
            final OutputStream compressedBuffer,
            final StreamingChannel streamingChannel
    ) {
        try (
                InputStream flowFileInputStream = session.read(flowFile);
                InputStream inputStream = getPreparedInputStream(flowFileInputStream)
        ) {
            if (FlowFileTrackingStatus.UNCOMMITTED == flowFileTrackingStatus) {
                final long bufferedRowCount = transferToBuffer(inputStream, inputBytes, compressedBuffer);
                bufferedRows.getAndAdd(bufferedRowCount);
            } else if (FlowFileTrackingStatus.REORDERED == flowFileTrackingStatus) {
                writeReorderedRowsToBuffer(session, flowFile, inputStream, compressedBuffer, bufferedRows, inputBytes, streamingChannel);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to read %s".formatted(flowFile), e);
        }
    }

    private InputStream getPreparedInputStream(final InputStream flowFileInputStream) throws IOException {
        final PushbackInputStream pushbackInputStream = new PushbackInputStream(flowFileInputStream, ZSTANDARD_MAGIC_NUMBER.length);
        final byte[] header = pushbackInputStream.readNBytes(ZSTANDARD_MAGIC_NUMBER.length);
        pushbackInputStream.unread(header);

        final InputStream preparedInputStream;

        if (Arrays.equals(ZSTANDARD_MAGIC_NUMBER, header)) {
            preparedInputStream = new ZstdInputStream(pushbackInputStream);
        } else {
            preparedInputStream = pushbackInputStream;
        }

        return preparedInputStream;
    }

    private OutputStream getCompressedOutputStream(final ByteArrayOutputStream buffer) {
        try {
            return new ZstdOutputStream(buffer);
        } catch (final IOException e) {
            throw new UncheckedIOException("Compressed Output Stream creation failed", e);
        }
    }

    private void closeOutputStream(final OutputStream outputStream) {
        try {
            outputStream.close();
        } catch (final IOException e) {
            throw new UncheckedIOException("Compressed Output Stream close failed", e);
        }
    }

    private void reportBufferedFlowFiles(
            final ProvenanceReporter provenanceReporter,
            final List<FlowFile> bufferedFlowFiles,
            final String transitUri,
            final AtomicLong bufferingStarted
    ) {
        final long started = bufferingStarted.get();
        final long elapsed = getDuration(started);
        for (final FlowFile bufferedFlowFile : bufferedFlowFiles) {
            provenanceReporter.send(bufferedFlowFile, transitUri, streamingUri.getHost(), elapsed);
        }
        bufferedFlowFiles.clear();
        bufferingStarted.set(System.nanoTime());
    }

    private void writeReorderedRowsToBuffer(
            final ProcessSession session,
            final FlowFile flowFile,
            final InputStream inputStream,
            final OutputStream buffer,
            final AtomicLong bufferedRows,
            final AtomicLong inputBytes,
            final StreamingChannel streamingChannel
    ) throws IOException {
        final byte[] inputBuffer = inputStream.readAllBytes();
        final BigDecimal committedOffsetToken = streamingChannel.getCommittedOffsetTokenNumber();
        long bufferedRowCount = 0;
        int startFragmentIndex = 0;

        long rowCount = 0;
        int startRowIndex = 0;
        int index = 0;
        boolean committedOffsetTokenFound = false;
        for (final byte character : inputBuffer) {
            if (LINE_FEED == character) {
                rowCount++;

                if (committedOffsetTokenFound) {
                    bufferedRowCount++;
                } else {
                    final int length = index - startRowIndex;
                    final JsonNode record = MAPPER.readTree(inputBuffer, startRowIndex, length);

                    final JsonNode offsetTokenNode = record.at(offsetTokenRecordPointer);
                    if (offsetTokenNode.isMissingNode()) {
                        final String flowFileId = flowFile.getAttribute(CoreAttributes.UUID.key());
                        throw new IllegalStateException("Offset Token not found in row [%d] FlowFile [%s]".formatted(rowCount, flowFileId));
                    }

                    final String offsetTokenString = offsetTokenNode.asText();
                    final BigDecimal rowOffsetToken = new BigDecimal(offsetTokenString);

                    if (rowOffsetToken.compareTo(committedOffsetToken) > 0) {
                        logger.debug("Found Row Offset Token [{}] greater than Committed Offset Token [{}] {}",
                                rowOffsetToken, committedOffsetToken, flowFile
                        );
                        committedOffsetTokenFound = true;
                        startFragmentIndex = startRowIndex;
                        bufferedRowCount++;
                    }

                    startRowIndex = index + 1;
                }
            }

            index++;
        }

        final int fragmentLength = index - startFragmentIndex;
        buffer.write(inputBuffer, startFragmentIndex, fragmentLength);

        inputBytes.getAndAdd(fragmentLength);
        bufferedRows.getAndAdd(bufferedRowCount);

        final long rowsSkipped = rowCount - bufferedRowCount;
        session.adjustCounter(SessionCounter.ROWS_SKIPPED.counter, rowsSkipped, false);
    }

    private long transferToBuffer(final InputStream inputStream, final AtomicLong inputBytes, final OutputStream outputStream) throws IOException {
        long bufferedRowCount = 0;
        byte lastCharacter = 0;

        final byte[] inputBuffer = new byte[INPUT_BUFFER_READ_SIZE];
        int read = inputStream.read(inputBuffer);
        while (read != END_OF_STREAM) {
            if (read != 0) {
                final long readRowCount = getBufferedRowCount(inputBuffer, read);
                bufferedRowCount += readRowCount;
                lastCharacter = inputBuffer[read - 1];
                outputStream.write(inputBuffer, 0, read);
                inputBytes.getAndAdd(read);
            }

            read = inputStream.read(inputBuffer);
        }

        final long rowCount;
        if (LINE_FEED == lastCharacter) {
            rowCount = bufferedRowCount;
        } else {
            outputStream.write(LINE_FEED);
            rowCount = bufferedRowCount + 1;
            inputBytes.getAndIncrement();
        }

        return rowCount;
    }

    private long getBufferedRowCount(final byte[] buffer, final int length) {
        long bufferedRowCount = 0;

        for (int i = 0; i < length; i++) {
            if (LINE_FEED == buffer[i]) {
                bufferedRowCount++;
            }
        }

        return bufferedRowCount;
    }

    private FlowFileTrackingStatus getFlowFileTrackingStatus(final FlowFileOffsetStatus flowFileOffsetStatus, final StreamingChannel streamingChannel) {
        final FlowFileTrackingStatus flowFileTrackingStatus;

        if (streamingChannel.getCommittedOffsetToken() == null) {
            flowFileTrackingStatus = FlowFileTrackingStatus.UNCOMMITTED;
        } else {
            final BigDecimal committedOffsetTokenNumber = streamingChannel.getCommittedOffsetTokenNumber();
            flowFileTrackingStatus = getFlowFileTrackingStatus(flowFileOffsetStatus, committedOffsetTokenNumber);
        }

        return flowFileTrackingStatus;
    }

    private FlowFileTrackingStatus getFlowFileTrackingStatus(final FlowFileOffsetStatus flowFileOffsetStatus, final BigDecimal committedOffsetTokenNumber) {
        final FlowFileTrackingStatus flowFileTrackingStatus;

        if (OffsetTrackingResolution.RECORD == offsetTrackingResolution) {
            if (committedOffsetTokenNumber.compareTo(flowFileOffsetStatus.endOffsetToken) >= 0) {
                flowFileTrackingStatus = FlowFileTrackingStatus.COMMITTED;
            } else if (committedOffsetTokenNumber.compareTo(flowFileOffsetStatus.startOffsetToken) < 0) {
                flowFileTrackingStatus = FlowFileTrackingStatus.UNCOMMITTED;
            } else {
                flowFileTrackingStatus = FlowFileTrackingStatus.REORDERED;
            }
        } else if (OffsetTrackingResolution.FLOW_FILE == offsetTrackingResolution) {
            if (committedOffsetTokenNumber.compareTo(flowFileOffsetStatus.endOffsetToken) >= 0) {
                flowFileTrackingStatus = FlowFileTrackingStatus.COMMITTED;
            } else {
                flowFileTrackingStatus = FlowFileTrackingStatus.UNCOMMITTED;
            }
        } else {
            flowFileTrackingStatus = FlowFileTrackingStatus.UNCOMMITTED;
        }

        return flowFileTrackingStatus;
    }

    private boolean isFileFragmentsTransferRequired(final int compressedRowsLength) {
        final boolean fileFragmentsTransferRequired;
        if (TransferStrategy.FILE_FRAGMENTS == transferStrategy) {
            fileFragmentsTransferRequired = true;
        } else if (TransferStrategy.MANAGED == transferStrategy) {
            fileFragmentsTransferRequired = compressedRowsLength >= ROWS_MAXIMUM_CONTENT_LENGTH;
        } else {
            fileFragmentsTransferRequired = false;
        }
        return fileFragmentsTransferRequired;
    }

    private void insertBuffer(
            final ProcessSession session,
            final AtomicLong inputBytes,
            final ByteArrayOutputStream buffer,
            final AtomicLong bufferedRows,
            final String flowFileOffsetToken,
            final StreamingChannel streamingChannel
    ) {
        final byte[] compressedRows = buffer.toByteArray();
        buffer.reset();

        final int compressedRowsLength = compressedRows.length;
        final boolean fileFragmentsTransferRequired = isFileFragmentsTransferRequired(compressedRowsLength);
        if (fileFragmentsTransferRequired) {
            insertFileFragments(session, inputBytes, compressedRows, bufferedRows, flowFileOffsetToken, streamingChannel);
        } else {
            insertRows(session, inputBytes, compressedRows, bufferedRows, flowFileOffsetToken, streamingChannel);
        }

        final long rowsSent = bufferedRows.get();
        session.adjustCounter(SessionCounter.ROWS_SENT.counter, rowsSent, false);

        bufferedRows.set(0);
    }

    private void insertRows(
            final ProcessSession session,
            final AtomicLong inputBytes,
            final byte[] compressedRows,
            final AtomicLong bufferedRows,
            final String offsetToken,
            final StreamingChannel streamingChannel
    ) {
        final long bytes = inputBytes.get();
        inputBytes.set(0);

        final int compressedBytes = compressedRows.length;

        final UUID requestId = UUID.randomUUID();
        final InsertOperation insertOperation = new RetryableInsertOperation(channelInsertTimeout, logger, streamingChannel, () -> {
            final String database = streamingChannel.getDatabase();
            final String schema = streamingChannel.getSchema();
            final String pipe = streamingChannel.getPipe();
            final String channel = streamingChannel.getChannel();
            final String continuationToken = streamingChannel.getContinuationToken();
            return streamingChannelClient.insertRows(database, schema, pipe, channel, continuationToken, offsetToken, compressedRows, bytes, requestId);
        });

        processInsertOperation(insertOperation, streamingChannel, offsetToken, bufferedRows.get(), bytes, compressedBytes, requestId);

        session.adjustCounter(SessionCounter.BATCHES_SENT.counter, 1, false);
        session.adjustCounter(SessionCounter.BYTES_SENT.counter, bytes, false);
        session.adjustCounter(SessionCounter.COMPRESSED_BYTES_SENT.counter, compressedBytes, false);
    }

    private void insertFileFragments(
            final ProcessSession session,
            final AtomicLong inputBytes,
            final byte[] compressedRows,
            final AtomicLong bufferedRows,
            final String offsetToken,
            final StreamingChannel streamingChannel
    ) {
        final String database = streamingChannel.getDatabase();
        final String schema = streamingChannel.getSchema();
        final String pipe = streamingChannel.getPipe();
        final String channel = streamingChannel.getChannel();
        final StreamingDestination streamingDestination = new StreamingDestination(database, schema, pipe, channel);
        final PipeInfo pipeInfo = getPipeInfo(streamingDestination, session);
        final ObjectTransferLocation objectTransferLocation = getObjectTransferLocation(pipeInfo);

        final long uncompressedBytes = inputBytes.get();
        inputBytes.set(0);

        final long rows = bufferedRows.get();

        FileFragmentInfo fileFragmentInfo;
        try {
            fileFragmentInfo = objectTransferClient.putFileFragment(compressedRows, rows, uncompressedBytes, objectTransferLocation, offsetToken);
        } catch (final RuntimeException e) {
            logger.warn("{} Failed to send File Fragment with Rows [{}] Offset Token [{}] retrying after {} ms",
                    streamingChannel, rows, offsetToken, FILE_FRAGMENT_RETRY_INTERVAL, e
            );
            try {
                TimeUnit.MILLISECONDS.sleep(FILE_FRAGMENT_RETRY_INTERVAL);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw e;
            }
            fileFragmentInfo = objectTransferClient.putFileFragment(compressedRows, rows, uncompressedBytes, objectTransferLocation, offsetToken);
        }

        final UUID requestId = UUID.randomUUID();
        insertFileFragmentInfo(fileFragmentInfo, streamingChannel, requestId);

        final long compressedBytes = compressedRows.length;
        final String filePath = fileFragmentInfo.filePath();
        logger.info("{} Sent Rows [{}] Compressed Bytes [{}] Offset Token [{}] File Path [{}] Request ID [{}]",
                streamingChannel, rows, compressedBytes, offsetToken, filePath, requestId
        );

        session.adjustCounter(SessionCounter.FILE_FRAGMENTS_SENT.counter, 1, false);
        session.adjustCounter(SessionCounter.BYTES_SENT.counter, uncompressedBytes, false);
        session.adjustCounter(SessionCounter.COMPRESSED_BYTES_SENT.counter, compressedBytes, false);
    }

    private void insertFileFragmentInfo(final FileFragmentInfo fileFragmentInfo, final StreamingChannel streamingChannel, final UUID requestId) {
        final InsertOperation insertOperation = new RetryableInsertOperation(channelInsertTimeout, logger, streamingChannel, () -> {
            final String database = streamingChannel.getDatabase();
            final String schema = streamingChannel.getSchema();
            final String pipe = streamingChannel.getPipe();
            final String channel = streamingChannel.getChannel();
            final String continuationToken = streamingChannel.getContinuationToken();
            return streamingChannelClient.insertFileFragments(database, schema, pipe, channel, continuationToken, List.of(fileFragmentInfo), requestId);
        });

        final long bytes = fileFragmentInfo.fragmentLengthUncompressedBytes();
        final long compressedBytes = fileFragmentInfo.fragmentLengthBytes();
        final long rows = fileFragmentInfo.fragmentRowCount();
        processInsertOperation(insertOperation, streamingChannel, fileFragmentInfo.fragmentEndOffsetToken(), rows, bytes, compressedBytes, requestId);
    }

    private void processInsertOperation(
            final InsertOperation insertOperation,
            final StreamingChannel streamingChannel,
            final String offsetToken,
            final long rows,
            final long bytes,
            final long compressedBytes,
            final UUID requestId
    ) {
        final InsertStatus insertStatus = insertOperation.insert();

        final long statusCode = insertStatus.statusCode();
        final String continuationToken = streamingChannel.getContinuationToken();
        logger.info("{} Inserted Rows [{}] Bytes [{}] Compressed [{}] Offset Token [{}] Continuation Token [{}] Status [{}] Request ID [{}]",
                streamingChannel, rows, bytes, compressedBytes, offsetToken, continuationToken, statusCode, requestId
        );

        final String nextContinuationToken = insertStatus.nextContinuationToken();
        streamingChannel.setContinuationToken(nextContinuationToken);
        streamingChannel.setOffsetToken(offsetToken);
    }

    private FlowFileOffsetStatus getFlowFileOffsetStatus(final ProcessContext context, final FlowFile flowFile) {
        final BigDecimal endOffsetToken;

        if (OffsetTrackingResolution.DISABLED == offsetTrackingResolution) {
            // Set End Offset Token from current timestamp in milliseconds for Disabled Offset Tracking
            final long now = System.currentTimeMillis();
            endOffsetToken = new BigDecimal(now);
        } else {
            final String endOffsetTokenProperty = context.getProperty(OFFSET_TOKEN_END_EXPRESSION).evaluateAttributeExpressions(flowFile).getValue();
            try {
                endOffsetToken = new BigDecimal(endOffsetTokenProperty);
            } catch (final NumberFormatException e) {
                throw new IllegalStateException("Offset Token End [%s] not a valid number %s".formatted(endOffsetTokenProperty, flowFile), e);
            }
        }

        final BigDecimal startOffsetToken;
        if (OffsetTrackingResolution.RECORD == offsetTrackingResolution) {
            final String startOffsetTokenProperty = context.getProperty(OFFSET_TOKEN_START_EXPRESSION).evaluateAttributeExpressions(flowFile).getValue();
            try {
                startOffsetToken = new BigDecimal(startOffsetTokenProperty);
            } catch (final NumberFormatException e) {
                throw new IllegalStateException("Offset Token Start [%s] not a valid number %s".formatted(startOffsetTokenProperty, flowFile), e);
            }
        } else {
            startOffsetToken = endOffsetToken;
        }

        return new FlowFileOffsetStatus(startOffsetToken, endOffsetToken);
    }

    private PipeInfo getPipeInfo(final StreamingDestination streamingDestination, final ProcessSession session) {
        final PipeInfo pipeInfo;

        final PipeInfo cachedPipeInfo = pipeInfos.get(streamingDestination);
        if (isPipeInfoExpired(cachedPipeInfo)) {
            final UUID requestId = UUID.randomUUID();
            pipeInfo = streamingChannelClient.getPipeInfo(streamingDestination.database(), streamingDestination.schema(), streamingDestination.pipe(), requestId);
            session.adjustCounter(SessionCounter.PIPE_INFO_REQUESTS.counter, 1, false);

            final ObjectTransferLocation objectTransferLocation = getObjectTransferLocation(pipeInfo);
            final String location = objectTransferLocation.stageLocation().location();
            logger.debug("Retrieved Pipe Info Location [{}] for {} Request ID [{}]", location, streamingDestination, requestId);
            pipeInfos.put(streamingDestination, pipeInfo);
        } else {
            pipeInfo = cachedPipeInfo;
        }

        return pipeInfo;
    }

    private ObjectTransferLocation getObjectTransferLocation(final PipeInfo pipeInfo) {
        final RowsetStageLocation durableStageLocation = pipeInfo.durableStageLocation();
        final RowsetStageLocation stageLocation;
        if (ChannelType.ELASTIC == channelType && durableStageLocation != null) {
            stageLocation = durableStageLocation;
        } else {
            stageLocation = pipeInfo.rowsetStageLocation();
        }

        return new ObjectTransferLocation(stageLocation, pipeInfo.pipeEncryptionInfo());
    }

    private boolean isPipeInfoExpired(final PipeInfo pipeInfo) {
        final boolean expired;

        if (pipeInfo == null) {
            expired = true;
        } else {
            // Calculate expired status from Rowset Stage Location regardless of Channel Type
            final RowsetStageLocation rowsetStageLocation = pipeInfo.rowsetStageLocation();
            final long expiryTimeMs = rowsetStageLocation.expiryTimeMs();
            final long now = System.currentTimeMillis();
            expired = now >= expiryTimeMs;
        }

        return expired;
    }

    private long getDuration(final long started) {
        final long elapsed = System.nanoTime() - started;
        final Duration duration = Duration.ofNanos(elapsed);
        return duration.toMillis();
    }

    private GenericKeyedObjectPoolConfig<StreamingChannel> getPoolConfig() {
        final GenericKeyedObjectPoolConfig<StreamingChannel> poolConfig = new GenericKeyedObjectPoolConfig<>();
        poolConfig.setMaxTotalPerKey(1);
        poolConfig.setMaxIdlePerKey(1);
        return poolConfig;
    }

    record FlowFileOffsetStatus(
            BigDecimal startOffsetToken,
            BigDecimal endOffsetToken
    ) {

    }

    enum FlowFileTrackingStatus {
        UNCOMMITTED,
        COMMITTED,
        REORDERED
    }

    enum SessionCounter {
        OFFSET_TOKEN_POLLING_DURATION("Offset Token Polling Duration"),
        PROCESSING_DURATION("Processing Duration"),
        PIPE_INFO_REQUESTS("Pipe Info Requests"),
        FLOW_FILES_SKIPPED("FlowFiles Skipped"),
        ROWS_SKIPPED("Rows Skipped"),
        ROWS_SENT("Rows Sent"),
        BATCHES_SENT("Batches Sent"),
        FILE_FRAGMENTS_SENT("File Fragments Sent"),
        BYTES_SENT("Bytes Sent"),
        COMPRESSED_BYTES_SENT("Compressed Bytes Sent");

        final String counter;

        SessionCounter(final String counter) {
            this.counter = counter;
        }
    }
}
