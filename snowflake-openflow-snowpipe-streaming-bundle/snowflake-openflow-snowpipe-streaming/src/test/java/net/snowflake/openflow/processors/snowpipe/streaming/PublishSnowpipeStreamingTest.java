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
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.MediaType;
import net.snowflake.openflow.components.snowpipe.streaming.StreamingHeader;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.ChannelStatusCode;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.InsertStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.LocationType;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.OpenedChannel;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeEncryptionInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.RowsetStageLocation;
import net.snowflake.openflow.components.snowpipe.streaming.security.EncryptionVersion;
import net.snowflake.openflow.components.snowpipe.streaming.transfer.S3CredentialProperty;
import net.snowflake.openflow.processors.snowpipe.streaming.connection.AuthenticationStrategy;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ChannelType;
import net.snowflake.openflow.processors.snowpipe.streaming.property.DestinationType;
import net.snowflake.openflow.processors.snowpipe.streaming.property.OffsetTrackingResolution;
import net.snowflake.openflow.processors.snowpipe.streaming.property.TransferStrategy;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okio.ByteString;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import static net.snowflake.openflow.processors.snowpipe.streaming.StreamingDestinationProvider.CHANNEL_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@Timeout(10)
@ExtendWith(MockitoExtension.class)
class PublishSnowpipeStreamingTest {
    private static final String KEY_ALGORITHM = "RSA";

    private static final String CHANNEL_INSERT_TIMEOUT = "5 s";

    private static final Duration WEB_CLIENT_TIMEOUT = Duration.ofSeconds(5);

    private static final String PRIVATE_KEY_SERVICE_ID = PrivateKeyService.class.getName();

    private static final String WEB_CLIENT_SERVICE_PROVIDER_ID = WebClientServiceProvider.class.getName();

    private static final String ACCOUNT = "localhost";

    private static final String USER = "STREAMING";

    private static final String DATABASE = "OPENFLOW";

    private static final String SCHEMA = "RUNTIME";

    private static final String PIPE = "SNOWPIPE";

    private static final String DEFAULT_PIPE = "SNOWPIPE-STREAMING";

    private static final String BASE_PATH = "/";

    private static final String STREAMING_HOSTNAME_PATH = "/v2/streaming/hostname";

    private static final String OAUTH_TOKEN_PATH = "/oauth/token";

    private static final String STREAMING_HOSTNAME_FORMAT = "http://%s:%d";

    private static final String REQUEST_ID_PARAMETER = "requestId";

    private static final byte[] EMPTY_ROWS = new byte[]{};

    private static final String NDJSON_ROW = "{\"ID\":1}\n";

    private static final String NDJSON_ROWS = "{\"ID\":1}\n{\"ID\":2}\n{\"ID\":3}\n";

    private static final String NDJSON_ROWS_FILTERED = "{\"ID\":2}\n{\"ID\":3}\n";

    private static final int NDJSON_ROW_COMPRESSED_BYTES = Zstd.compress(NDJSON_ROW.getBytes(StandardCharsets.UTF_8)).length;

    private static final int NDJSON_ROWS_FILTERED_COMPRESSED_BYTES = Zstd.compress(NDJSON_ROWS_FILTERED.getBytes(StandardCharsets.UTF_8)).length;

    private static final int ROWS_ERROR_COUNT = 1;

    private static final String PARSING_FAILED_MESSAGE = "Parsing Failed";

    private static final String SCOPED_TOKEN = "SCOPED";

    private static final String OFFSET_TRACKING_TIMEOUT = "5 s";

    private static final String FIRST_CONTINUATION_TOKEN = "0_1";

    private static final String SECOND_CONTINUATION_TOKEN = "0_2";

    private static final String FIRST_CHANNEL_NAME = "HOSTNAME-CHANNEL-0";

    private static final String ELASTIC_CHANNEL_NAME = "ELASTIC";

    private static final String OFFSET_TOKEN_ATTRIBUTE_NAME = "offsetToken";

    private static final String END_OFFSET_TOKEN_ATTRIBUTE_NAME = "endOffsetToken";

    private static final String FIRST_OFFSET_TOKEN = "1";

    private static final String THIRD_OFFSET_TOKEN = "3";

    private static final String OFFSET_TOKEN_EXPRESSION = "${offsetToken}";

    private static final String END_OFFSET_TOKEN_EXPRESSION = "${endOffsetToken}";

    private static final String OFFSET_TOKEN_RECORD_POINTER = "/ID";

    private static final String INVALID_REQUEST_STATUS_CODE = "ERR_INVALID_REQUEST";

    private static final String ZSTD_ENCODING = "zstd";

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .build();

    private static final OpenedChannel CREATED_OPENED_CHANNEL = new OpenedChannel(
            FIRST_CONTINUATION_TOKEN,
            getChannelStatus(null, ChannelStatusCode.SUCCESS.getStatus())
    );

    private static final OpenedChannel REOPENED_CHANNEL = new OpenedChannel(
            FIRST_CONTINUATION_TOKEN,
            getChannelStatus(FIRST_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus())
    );

    private static final int PIPE_KEY_LENGTH = 16;

    private static final String PIPE_KEY_ALGORITHM = "AES";

    private static final String CIPHER_ALGORITHM = "AES/CTR/NoPadding";

    private static final String DIVERSIFIER = "01";

    private static final Base64.Encoder encoder = Base64.getEncoder();

    private static final String STAGE_LOCATION_BUCKET = "staging";

    private static final String STAGE_LOCATION_FORMAT = "%s/holding/";

    private static final String STAGE_LOCATION_OBJECT_PATH = "/holding/";

    private static final String DURABLE_STAGE_LOCATION_FORMAT = "%s/durable/";

    private static final String DURABLE_STAGE_LOCATION_OBJECT_PATH = "/durable/";

    private static final String STAGE_LOCATION_REGION = "region";

    private static final String S3_HOST = "%s.s3.%s.amazonaws.com".formatted(STAGE_LOCATION_BUCKET, STAGE_LOCATION_REGION);

    private static final String ENTITY_TAG = "Entity-Tag";

    private static final String ENTITY_TAG_HEADER = "\"%s\"".formatted(ENTITY_TAG);

    private static final String PIPE_KEY = getPipeKey();

    private static final PipeEncryptionInfo PIPE_ENCRYPTION_INFO = new PipeEncryptionInfo(
            PIPE_KEY,
            0,
            DIVERSIFIER,
            EncryptionVersion.AES_CTR_128_V1.getVersion()
    );

    private static final PipeInfo PIPE_INFO = new PipeInfo(
            0,
            null,
            getStageLocation(STAGE_LOCATION_FORMAT.formatted(STAGE_LOCATION_BUCKET)),
            PIPE_ENCRYPTION_INFO,
            null
    );

    private static final PipeInfo PIPE_INFO_DURABLE_STAGE_LOCATION = new PipeInfo(
            0,
            null,
            getStageLocation(STAGE_LOCATION_FORMAT.formatted(STAGE_LOCATION_BUCKET)),
            PIPE_ENCRYPTION_INFO,
            getStageLocation(DURABLE_STAGE_LOCATION_FORMAT.formatted(STAGE_LOCATION_BUCKET))
    );

    private static final String ANNOTATION_DATA = PublishSnowpipeStreamingTest.class.getSimpleName();

    private static PrivateKey privateKey;

    @StartStop
    public final MockWebServer server = new MockWebServer();

    private URI accountUri;

    @Mock
    private PrivateKeyService privateKeyService;

    @Mock
    private WebClientServiceProvider webClientServiceProvider;

    private StandardWebClientService standardWebClientService;

    private TestRunner runner;

    @BeforeAll
    static void setPrivateKey() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        privateKey = keyPair.getPrivate();
    }

    @BeforeEach
    void setRunner() throws Exception {
        final HttpUrl baseUrl = server.url(BASE_PATH);
        accountUri = baseUrl.uri();

        final PublishSnowpipeStreaming processor = new PublishSnowpipeStreaming();
        processor.streamingClientProvider = new LocalStreamingClientProvider();
        runner = TestRunners.newTestRunner(processor);

        when(privateKeyService.getIdentifier()).thenReturn(PRIVATE_KEY_SERVICE_ID);
        runner.addControllerService(PRIVATE_KEY_SERVICE_ID, privateKeyService);
        runner.enableControllerService(privateKeyService);
        lenient().when(privateKeyService.getPrivateKey()).thenReturn(privateKey);

        when(webClientServiceProvider.getIdentifier()).thenReturn(WEB_CLIENT_SERVICE_PROVIDER_ID);
        runner.addControllerService(WEB_CLIENT_SERVICE_PROVIDER_ID, webClientServiceProvider);
        runner.enableControllerService(webClientServiceProvider);
        standardWebClientService = new LocalStandardWebServiceClient();
        standardWebClientService.setConnectTimeout(WEB_CLIENT_TIMEOUT);
        standardWebClientService.setReadTimeout(WEB_CLIENT_TIMEOUT);
        lenient().when(webClientServiceProvider.getWebClientService()).thenReturn(standardWebClientService);

        setStandardProperties();

        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TRACKING_RESOLUTION, OffsetTrackingResolution.RECORD);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TRACKING_TIMEOUT, OFFSET_TRACKING_TIMEOUT);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TOKEN_START_EXPRESSION, OFFSET_TOKEN_EXPRESSION);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TOKEN_END_EXPRESSION, OFFSET_TOKEN_EXPRESSION);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TOKEN_RECORD_POINTER, OFFSET_TOKEN_RECORD_POINTER);

        // Set Annotation Data for User-Agent Header
        runner.setAnnotationData(ANNOTATION_DATA);
    }

    private void setStandardProperties() {
        runner.setProperty(StandardStreamingClientProvider.AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR);
        runner.setProperty(StandardStreamingClientProvider.ACCOUNT, ACCOUNT);
        runner.setProperty(StandardStreamingClientProvider.USER, USER);
        runner.setProperty(StandardStreamingClientProvider.PRIVATE_KEY_SERVICE, PRIVATE_KEY_SERVICE_ID);
        runner.setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.PIPE);
        runner.setProperty(StreamingDestinationProvider.DATABASE, DATABASE);
        runner.setProperty(StreamingDestinationProvider.SCHEMA, SCHEMA);
        runner.setProperty(StreamingDestinationProvider.PIPE, PIPE);
        runner.setProperty(StandardStreamingClientProvider.WEB_CLIENT_SERVICE_PROVIDER, WEB_CLIENT_SERVICE_PROVIDER_ID);
        runner.setProperty(PublishSnowpipeStreaming.CHANNEL_INSERT_TIMEOUT, CHANNEL_INSERT_TIMEOUT);
    }

    @AfterEach
    void closeWebClientService() {
        standardWebClientService.close();
    }

    @Test
    void testMigrateProperties() {
        runner.clearProperties();
        runner.setProperty(StreamingDestinationProvider.PIPE, PIPE);

        runner.migrateProperties();

        final ProcessContext processContext = runner.getProcessContext();
        final DestinationType destinationType = processContext.getProperty(StreamingDestinationProvider.DESTINATION_TYPE).asAllowableValue(DestinationType.class);
        assertEquals(DestinationType.PIPE, destinationType);
    }

    @Test
    void testMigratePropertiesDefaultPipe() {
        runner.clearProperties();
        runner.setProperty(StreamingDestinationProvider.PIPE, DEFAULT_PIPE);

        runner.migrateProperties();

        final ProcessContext processContext = runner.getProcessContext();
        final DestinationType destinationType = processContext.getProperty(StreamingDestinationProvider.DESTINATION_TYPE).asAllowableValue(DestinationType.class);
        assertEquals(DestinationType.TABLE, destinationType);

        final String table = processContext.getProperty(StreamingDestinationProvider.TABLE).evaluateAttributeExpressions(Map.of()).getValue();
        assertEquals(PIPE, table);
    }

    @Test
    void testVerifyFailed() {
        server.enqueue(new MockResponse.Builder().code(HttpURLConnection.HTTP_UNAUTHORIZED).build());

        final List<ConfigVerificationResult> results = runner.verify(Map.of());
        final ConfigVerificationResult.Outcome failedOutcome = findFailedOutcome(results);
        assertNotNull(failedOutcome);

        assertStreamingHostnameRequestRecorded();
    }

    @Test
    void testVerifySuccess() {
        enqueueStreamingHostname();

        final List<ConfigVerificationResult> results = runner.verify(Map.of());
        final ConfigVerificationResult.Outcome failedOutcome = findFailedOutcome(results);
        assertNull(failedOutcome);

        assertStreamingHostnameRequestRecorded();
    }

    @Test
    void testRunNoFlowFiles() {
        enqueueStreamingHostname();

        runner.run();

        final Set<Relationship> relationships = runner.getProcessor().getRelationships();
        for (final Relationship relationship : relationships) {
            final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(relationship);
            assertTrue(flowFiles.isEmpty(), "FlowFiles found for Relationship [%s]".formatted(relationship));
        }

        assertStreamingHostnameRequestRecorded();
    }

    @Test
    void testRunOpenChannelFailed() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        server.enqueue(new MockResponse.Builder().code(HttpURLConnection.HTTP_BAD_REQUEST).build());

        runner.enqueue(NDJSON_ROW);
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.FAILURE);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
    }

    @Test
    void testRunEmptyFlowFileSuccess() {
        enqueueStreamingHostname();

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        runner.enqueue(EMPTY_ROWS);
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.EMPTY);

        final List<MockFlowFile> emptyFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.EMPTY);
        assertEquals(1, emptyFlowFiles.size(), "Expected one FlowFile in EMPTY");

        final MockFlowFile emptyFlowFile = emptyFlowFiles.getFirst();
        assertEquals(0, emptyFlowFile.getSize(), "Expected empty FlowFile content");

        assertStreamingHostnameRequestRecorded();
        final RecordedRequest recordedRequest = takeRequest();
        assertNull(recordedRequest, "No additional requests should be made for empty FlowFiles");
    }

    @Test
    void testRunInsertRowsSuccess() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatus(FIRST_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(channelStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends data, registers pending batch
        runner.run(1, false, true);

        // FlowFiles should not yet be routed to SUCCESS (pending batch)
        final List<MockFlowFile> successAfterFirstRun = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.SUCCESS);
        assertEquals(0, successAfterFirstRun.size(), "Expected no FlowFiles in SUCCESS after first run");

        // Second run: checks pending batch, routes to SUCCESS
        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.SUCCESS);
        assertEquals(1, successFlowFiles.size(), "Expected one FlowFile in SUCCESS after second run");

        final MockFlowFile successFlowFile = successFlowFiles.getFirst();
        successFlowFile.assertAttributeEquals(ChannelStatusAttributes.CHANNEL_STATUS_ROW_ERRORS_ADDED, "0");

        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BATCHES_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.ROWS_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BYTES_SENT.counter, NDJSON_ROW.length());
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.COMPRESSED_BYTES_SENT.counter, NDJSON_ROW_COMPRESSED_BYTES);
        assertDurationCountersFound();

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROW, FIRST_CONTINUATION_TOKEN);
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunPendingBatchCachedChannelStatusYields() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueueInsertStatus();

        final ChannelStatus uncommittedStatus = getChannelStatus(null, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(uncommittedStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        runner.run(1, false, true);
        assertFalse(runner.isYieldCalled());
        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS, 0);

        runner.run(1, true, false);
        assertTrue(runner.isYieldCalled());
        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS, 0);
    }

    @Test
    void testRunInsertRowsSuccessAfterServiceUnavailableBulkStatus() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueueInsertStatus();

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);
        runner.run(1, false, true);
        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS, 0);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROW, FIRST_CONTINUATION_TOKEN);

        // Pending Batch checks Bulk Channel Status and receives HTTP 503
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_UNAVAILABLE)
                        .build()
        );
        runner.run(1, false, false);
        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS, 0);
        assertBulkChannelStatusRequestRecorded();

        final MockComponentLog logger = runner.getLogger();
        final List<LogMessage> warnMessages = logger.getWarnMessages();
        assertTrue(warnMessages.isEmpty(), "Warning Log Messages found");

        // Informational message should be logged with HTTP 503
        final List<LogMessage> infoMessages = runner.getLogger().getInfoMessages();
        final Optional<LogMessage> exceptionInfoMessage = infoMessages.stream().filter(logMessage -> logMessage.getThrowable() instanceof HttpResponseException).findFirst();
        assertTrue(exceptionInfoMessage.isPresent());
        final HttpResponseException httpResponseException = (HttpResponseException) exceptionInfoMessage.get().getThrowable();
        assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, httpResponseException.getStatusCode());

        // Pending Batch checks Bulk Channel Status and receives HTTP 200
        final ChannelStatus channelStatus = getChannelStatus(FIRST_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(channelStatus);

        runner.run(1, true, false);
        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS, 1);
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunInsertRowsEmptyElasticChannelType() {
        enqueueStreamingHostname();

        runner.setProperty(CHANNEL_TYPE, ChannelType.ELASTIC);
        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        runner.enqueue(EMPTY_ROWS);

        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.EMPTY);

        assertStreamingHostnameRequestRecorded();
    }

    @Test
    void testRunInsertRowsSuccessElasticChannelType() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueInsertStatus();

        runner.setProperty(CHANNEL_TYPE, ChannelType.ELASTIC);
        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS);

        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BATCHES_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.ROWS_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BYTES_SENT.counter, NDJSON_ROW.length());
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.COMPRESSED_BYTES_SENT.counter, NDJSON_ROW_COMPRESSED_BYTES);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROW);
    }

    @Test
    void testRunInsertRowsSuccessFlowFileResolution() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatus(FIRST_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(channelStatus);

        runner.clearProperties();
        setStandardProperties();

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TRACKING_RESOLUTION, OffsetTrackingResolution.FLOW_FILE);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TOKEN_END_EXPRESSION, OFFSET_TOKEN_EXPRESSION);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends data
        runner.run(1, false, true);

        // Second run: checks commit status
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS);

        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BATCHES_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.ROWS_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BYTES_SENT.counter, NDJSON_ROW.length());
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.COMPRESSED_BYTES_SENT.counter, NDJSON_ROW_COMPRESSED_BYTES);
        assertDurationCountersFound();

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROW, FIRST_CONTINUATION_TOKEN);
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunInsertRowsSuccessReordered() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel(REOPENED_CHANNEL);
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatus(THIRD_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(channelStatus);

        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TOKEN_END_EXPRESSION, END_OFFSET_TOKEN_EXPRESSION);
        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN,
                END_OFFSET_TOKEN_ATTRIBUTE_NAME, THIRD_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROWS, attributes);

        // First run: sends filtered rows
        runner.run(1, false, true);

        // Second run: checks commit status
        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.SUCCESS);
        assertEquals(1, successFlowFiles.size(), "Expected one FlowFile in SUCCESS");

        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BATCHES_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.ROWS_SENT.counter, 2);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BYTES_SENT.counter, NDJSON_ROWS_FILTERED.length());
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.COMPRESSED_BYTES_SENT.counter, NDJSON_ROWS_FILTERED_COMPRESSED_BYTES);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.ROWS_SKIPPED.counter, 1);
        assertDurationCountersFound();

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROWS_FILTERED, FIRST_CONTINUATION_TOKEN);
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunInsertRowsFailed() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        server.enqueue(new MockResponse.Builder().code(HttpURLConnection.HTTP_BAD_REQUEST).build());

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.FAILURE);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROW, FIRST_CONTINUATION_TOKEN);
    }

    @Test
    void testRunInsertFileFragmentsSuccess() throws GeneralSecurityException {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueuePipeInfo();
        enqueuePutObject();
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatus(FIRST_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(channelStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends file fragment
        runner.run(1, false, true);

        // Second run: checks commit status
        runner.run(1, true, false);

        final List<MockFlowFile> successFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.SUCCESS);
        assertEquals(1, successFlowFiles.size(), "Expected one FlowFile in SUCCESS");

        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.FILE_FRAGMENTS_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.ROWS_SENT.counter, 1);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.BYTES_SENT.counter, NDJSON_ROW.length());
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.COMPRESSED_BYTES_SENT.counter, NDJSON_ROW_COMPRESSED_BYTES);
        assertCounterEquals(StandardSnowpipeStreamingService.SessionCounter.PIPE_INFO_REQUESTS.counter, 1);
        assertDurationCountersFound();

        final List<ProvenanceEventRecord> provenanceEventRecords = runner.getProvenanceEvents();
        assertEquals(1, provenanceEventRecords.size());
        final ProvenanceEventRecord provenanceEventRecord = provenanceEventRecords.getFirst();
        assertEquals(ProvenanceEventType.SEND, provenanceEventRecord.getEventType());
        assertNotNull(provenanceEventRecord.getDetails());
        assertNotNull(provenanceEventRecord.getTransitUri());

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertFileFragmentTransferred();
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunInsertFileFragmentsSuccessElasticChannelTypeDurableStageLocation() throws GeneralSecurityException {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueuePipeInfo(PIPE_INFO_DURABLE_STAGE_LOCATION);
        enqueuePutObject();
        enqueueInsertStatus();

        runner.setProperty(CHANNEL_TYPE, ChannelType.ELASTIC);
        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertFileFragmentTransferred(DURABLE_STAGE_LOCATION_OBJECT_PATH, ELASTIC_CHANNEL_NAME);
    }

    @Test
    void testRunInsertFileFragmentsSuccessElasticChannelTypeWithoutDurableStageLocation() throws GeneralSecurityException {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueuePipeInfo();
        enqueuePutObject();
        enqueueInsertStatus();

        runner.setProperty(CHANNEL_TYPE, ChannelType.ELASTIC);
        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertFileFragmentTransferred(STAGE_LOCATION_OBJECT_PATH, ELASTIC_CHANNEL_NAME);
    }

    @Test
    void testRunInsertFileFragmentsSuccessDurableStageLocationIgnored() throws GeneralSecurityException {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueuePipeInfo(PIPE_INFO_DURABLE_STAGE_LOCATION);
        enqueuePutObject();
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatus(FIRST_OFFSET_TOKEN, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(channelStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends file fragment
        runner.run(1, false, true);

        // Second run: checks commit status
        runner.run(1, true, false);

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.SUCCESS);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertFileFragmentTransferred(STAGE_LOCATION_OBJECT_PATH, FIRST_CONTINUATION_TOKEN);
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunInsertFileFragmentsFailed() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueuePipeInfo();
        enqueuePutObject();
        server.enqueue(new MockResponse.Builder().code(HttpURLConnection.HTTP_BAD_REQUEST).build());

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(PublishSnowpipeStreaming.FAILURE);

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertPutObjectRequestRecorded();
        assertInsertFileFragmentsRequestRecorded();
    }

    @Test
    void testRunInsertFileFragmentsInvalid() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueuePipeInfo();
        enqueuePutObject();
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatusErrorCount();
        enqueueChannelStatus(channelStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends file fragment
        runner.run(1, false, true);

        // Second run: checks commit status and detects invalid rows
        runner.run(1, true, false);

        final List<MockFlowFile> invalidFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.INVALID);
        assertEquals(1, invalidFlowFiles.size(), "Expected one FlowFile in INVALID");

        final MockFlowFile invalidFlowFile = invalidFlowFiles.getFirst();
        invalidFlowFile.assertAttributeEquals(ChannelStatusAttributes.CHANNEL_STATUS_ROW_ERRORS_ADDED, Integer.toString(ROWS_ERROR_COUNT));

        final List<LogMessage> warnMessages = runner.getLogger().getWarnMessages();
        assertFalse(warnMessages.isEmpty());
        final LogMessage firstWarnMessage = warnMessages.getFirst();
        assertTrue(firstWarnMessage.getMsg().contains(PARSING_FAILED_MESSAGE), "Channel Status Error Message not found");

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertPutObjectRequestRecorded();
        assertInsertFileFragmentsRequestRecorded();
    }

    @Test
    void testRunInsertFileFragmentsChannelStatusFailure() throws GeneralSecurityException {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueuePipeInfo();
        enqueuePutObject();
        enqueueInsertStatus();

        final ChannelStatus channelStatus = getChannelStatus(FIRST_OFFSET_TOKEN, INVALID_REQUEST_STATUS_CODE);
        enqueueChannelStatus(channelStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.FILE_FRAGMENTS);

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends file fragment
        runner.run(1, false, true);

        // Second run: checks commit status, finds failure status
        runner.run(1, true, false);

        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.FAILURE);
        assertEquals(1, failureFlowFiles.size(), "Expected one FlowFile in FAILURE");

        final MockFlowFile failureFlowFile = failureFlowFiles.getFirst();
        failureFlowFile.assertAttributeEquals(ChannelStatusAttributes.CHANNEL_STATUS_ROW_ERRORS_ADDED, "0");

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertPipeInfoRequestRecorded();
        assertFileFragmentTransferred();
        assertBulkChannelStatusRequestRecorded();
    }

    @Test
    void testRunOffsetTokenTimeout() {
        enqueueStreamingHostname();
        enqueueScopedToken();
        enqueueOpenedChannel();
        enqueueInsertStatus();

        // Enqueue channel status that never reports committed offset token
        final ChannelStatus uncommittedStatus = getChannelStatus(null, ChannelStatusCode.SUCCESS.getStatus());
        enqueueChannelStatus(uncommittedStatus);

        runner.setProperty(PublishSnowpipeStreaming.TRANSFER_STRATEGY, TransferStrategy.ROWS);
        runner.setProperty(PublishSnowpipeStreaming.OFFSET_TRACKING_TIMEOUT, "1 ms");

        final Map<String, String> attributes = Map.of(
                OFFSET_TOKEN_ATTRIBUTE_NAME, FIRST_OFFSET_TOKEN
        );
        runner.enqueue(NDJSON_ROW, attributes);

        // First run: sends data, registers pending batch
        runner.run(1, false, true);

        // Second run: pending batch times out, routes to FAILURE
        runner.run(1, true, false);

        final List<MockFlowFile> failureFlowFiles = runner.getFlowFilesForRelationship(PublishSnowpipeStreaming.FAILURE);
        assertEquals(1, failureFlowFiles.size(), "Expected one FlowFile in FAILURE after timeout");

        final MockFlowFile failureFlowFile = failureFlowFiles.getFirst();
        failureFlowFile.assertAttributeEquals(ChannelStatusAttributes.CHANNEL_STATUS_ROW_ERRORS_ADDED, "0");

        assertStreamingHostnameRequestRecorded();
        assertScopedTokenRequestRecorded();
        assertOpenChannelRequestRecorded();
        assertInsertRowsRequestRecorded(NDJSON_ROW, FIRST_CONTINUATION_TOKEN);
    }

    private void assertFileFragmentTransferred() throws GeneralSecurityException {
        assertFileFragmentTransferred(STAGE_LOCATION_OBJECT_PATH, FIRST_CONTINUATION_TOKEN);
    }

    private void assertFileFragmentTransferred(final String expectedObjectPath, final String expectedPathSegment) throws GeneralSecurityException {
        final byte[] putObjectRequestBody = assertPutObjectRequestRecorded(expectedObjectPath);
        final FileFragmentInfo fileFragmentInfo = assertInsertFileFragmentsRequestRecorded(expectedPathSegment);
        assertEquals(ENTITY_TAG, fileFragmentInfo.fileEtag());
        assertDecryptedFileFragmentEquals(putObjectRequestBody, fileFragmentInfo);
    }

    private void assertDurationCountersFound() {
        final Long offsetTokenPollingDuration = runner.getCounterValue(StandardSnowpipeStreamingService.SessionCounter.OFFSET_TOKEN_POLLING_DURATION.counter);
        assertNotNull(offsetTokenPollingDuration, "Counter [%s] not found".formatted(StandardSnowpipeStreamingService.SessionCounter.OFFSET_TOKEN_POLLING_DURATION.counter));

        final Long processingDuration = runner.getCounterValue(StandardSnowpipeStreamingService.SessionCounter.PROCESSING_DURATION.counter);
        assertNotNull(processingDuration, "Counter [%s] not found".formatted(StandardSnowpipeStreamingService.SessionCounter.PROCESSING_DURATION.counter));
    }

    private void assertCounterEquals(final String counter, final long expected) {
        final Long counterValue = runner.getCounterValue(counter);
        assertEquals(expected, counterValue);
    }

    private void assertStreamingHostnameRequestRecorded() {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.GET.toString(), recordedRequest.getMethod());
        assertEquals(STREAMING_HOSTNAME_PATH, recordedRequest.getTarget());
    }

    private void assertScopedTokenRequestRecorded() {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.POST.toString(), recordedRequest.getMethod());
        assertEquals(OAUTH_TOKEN_PATH, recordedRequest.getTarget());
    }

    private void assertOpenChannelRequestRecorded() {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.PUT.toString(), recordedRequest.getMethod());
        final String path = recordedRequest.getTarget();
        assertNotNull(path);
        assertTrue(path.contains(PIPE), "Pipe not found in path [%s]".formatted(path));
        assertTrue(path.contains(REQUEST_ID_PARAMETER), "Request ID parameter not found in path [%s]".formatted(path));

        final Headers headers = recordedRequest.getHeaders();
        final String userAgent = headers.get(HttpHeaderName.USER_AGENT.getHeaderName());
        assertNotNull(userAgent);

        final Runtime.Version runtimeVersion = Runtime.version();
        final String expectedVersion = runtimeVersion.toString();
        assertTrue(userAgent.contains(expectedVersion), "Runtime Version [%s] not found in User-Agent Header [%s]".formatted(expectedVersion, userAgent));

        assertTrue(userAgent.contains(ANNOTATION_DATA), "Annotation Data not found in User-Agent Header [%s]".formatted(userAgent));
    }

    private void assertInsertRowsRequestRecorded(final String expectedRows, final String... expectedPathSegments) {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.POST.toString(), recordedRequest.getMethod());
        final String path = recordedRequest.getTarget();
        assertNotNull(path);
        assertTrue(path.contains(REQUEST_ID_PARAMETER), "Request ID parameter not found in path [%s]".formatted(path));

        for (final String expectedPathSegment : expectedPathSegments) {
            assertTrue(path.contains(expectedPathSegment), "Path Segment [%s] not found in Request URI Path [%s]".formatted(expectedPathSegment, path));
        }

        final Headers requestHeaders = recordedRequest.getHeaders();
        final String contentEncoding = requestHeaders.get(HttpHeaderName.CONTENT_ENCODING.getHeaderName());
        assertEquals(ZSTD_ENCODING, contentEncoding);

        final String uncompressedContentLengthHeader = requestHeaders.get(StreamingHeader.UNCOMPRESSED_CONTENT_LENGTH.getHeaderName());
        assertEquals(Integer.toString(expectedRows.length()), uncompressedContentLengthHeader);

        final ByteString recordedRequestBody = recordedRequest.getBody();
        assertNotNull(recordedRequestBody);
        final byte[] requestBody = recordedRequestBody.toByteArray();
        final byte[] requestBodyDecompressed = Zstd.decompress(requestBody, expectedRows.length());
        final String rows = new String(requestBodyDecompressed);
        assertEquals(expectedRows, rows);
    }

    private void assertBulkChannelStatusRequestRecorded() {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.POST.toString(), recordedRequest.getMethod());
        final String path = recordedRequest.getTarget();
        assertNotNull(path);
        assertTrue(path.contains(PIPE), "Pipe Name not found in path [%s]".formatted(path));
        assertTrue(path.contains(REQUEST_ID_PARAMETER), "Request ID parameter not found in path [%s]".formatted(path));
    }

    private void assertPipeInfoRequestRecorded() {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.POST.toString(), recordedRequest.getMethod());
        final String path = recordedRequest.getTarget();
        assertNotNull(path);
        assertTrue(path.endsWith(EncryptionVersion.AES_CTR_128_V1.getVersion()), "Encryption Version not found in path [%s]".formatted(path));
        assertTrue(path.contains(REQUEST_ID_PARAMETER), "Request ID parameter not found in path [%s]".formatted(path));
    }

    private byte[] assertPutObjectRequestRecorded() {
        return assertPutObjectRequestRecorded(STAGE_LOCATION_OBJECT_PATH);
    }

    private byte[] assertPutObjectRequestRecorded(final String expectedObjectPath) {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.PUT.toString(), recordedRequest.getMethod());

        final Headers requestHeaders = recordedRequest.getHeaders();
        final String authorizationHeader = requestHeaders.get(HttpHeaderName.AUTHORIZATION.getHeaderName());
        assertNotNull(authorizationHeader, "Authorization Header not found");

        final String contentTypeHeader = requestHeaders.get(HttpHeaderName.CONTENT_TYPE.getHeaderName());
        assertEquals(MediaType.APPLICATION_NDJSON.getMediaType(), contentTypeHeader);

        final String path = recordedRequest.getTarget();
        assertNotNull(path, "Path not found");
        assertTrue(path.startsWith(expectedObjectPath), "Object Path [%s] not found in Request URI Path [%s]".formatted(expectedObjectPath, path));

        final ByteString recordedRequestBody = recordedRequest.getBody();
        assertNotNull(recordedRequestBody);
        final byte[] requestBody = recordedRequestBody.toByteArray();
        assertNotNull(requestBody, "Request Body not found");
        return requestBody;
    }

    private FileFragmentInfo assertInsertFileFragmentsRequestRecorded() {
        return assertInsertFileFragmentsRequestRecorded(FIRST_CONTINUATION_TOKEN);
    }

    private FileFragmentInfo assertInsertFileFragmentsRequestRecorded(final String expectedPathSegment) {
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals(HttpMethod.POST.toString(), recordedRequest.getMethod());
        final String path = recordedRequest.getTarget();
        assertNotNull(path);
        assertTrue(path.contains(expectedPathSegment), "Path Segment [%s] not found in Request URI Path [%s]".formatted(expectedPathSegment, path));
        assertTrue(path.contains(REQUEST_ID_PARAMETER), "Request ID parameter not found in path [%s]".formatted(path));

        final ByteString recordedRequestBody = recordedRequest.getBody();
        assertNotNull(recordedRequestBody);
        final byte[] requestBody = recordedRequestBody.toByteArray();
        final FileFragmentInfo[] fileFragmentInfos = MAPPER.readValue(requestBody, FileFragmentInfo[].class);
        return fileFragmentInfos[0];
    }

    private void assertDecryptedFileFragmentEquals(final byte[] requestBody, final FileFragmentInfo fileFragmentInfo) throws GeneralSecurityException {
        final String ivBase64 = fileFragmentInfo.ivBase64();
        final byte[] iv = Base64.getDecoder().decode(ivBase64);

        final byte[] pipeKey = Base64.getDecoder().decode(PIPE_KEY);
        final SecretKey secretKey = new SecretKeySpec(pipeKey, PIPE_KEY_ALGORITHM);
        final IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

        final Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec);

        final byte[] decrypted = cipher.doFinal(requestBody);
        final byte[] decompressed = Zstd.decompress(decrypted, decrypted.length);
        final String row = new String(decompressed, StandardCharsets.UTF_8);
        assertEquals(NDJSON_ROW, row);
    }

    private RecordedRequest takeRequest() {
        try {
            return server.takeRequest(500, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Recorded Request not found");
        }
    }

    private void enqueueStreamingHostname() {
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .body(accountUri.getHost())
                        .build()
        );
    }

    private void enqueueScopedToken() {
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .body(SCOPED_TOKEN)
                        .build()
        );
    }

    private void enqueueOpenedChannel() {
        enqueueOpenedChannel(CREATED_OPENED_CHANNEL);
    }

    private void enqueueOpenedChannel(final OpenedChannel openedChannel) {
        final String body = getSerializedObject(openedChannel);
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .body(body)
                        .build()
        );
    }

    private void enqueueInsertStatus() {
        final InsertStatus insertStatus = new InsertStatus(0, null, SECOND_CONTINUATION_TOKEN);
        final String body = getSerializedObject(insertStatus);
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .body(body)
                        .build()
        );
    }

    private void enqueueChannelStatus(final ChannelStatus channelStatus) {
        final Map<String, ChannelStatus> channelStatuses = Map.of(channelStatus.channelName(), channelStatus);
        final BulkChannelStatus bulkChannelStatus = new BulkChannelStatus(channelStatuses);
        final String body = getSerializedObject(bulkChannelStatus);
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .body(body)
                        .build()
        );
    }

    private void enqueuePipeInfo() {
        enqueuePipeInfo(PIPE_INFO);
    }

    private void enqueuePipeInfo(final PipeInfo pipeInfo) {
        final String body = getSerializedObject(pipeInfo);
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .body(body)
                        .build()
        );
    }

    private void enqueuePutObject() {
        server.enqueue(
                new MockResponse.Builder()
                        .code(HttpURLConnection.HTTP_OK)
                        .addHeader(HttpHeaderName.ETAG.getHeaderName(), ENTITY_TAG_HEADER)
                        .build()
        );
    }

    private String getSerializedObject(final Object object) {
        return MAPPER.writeValueAsString(object);
    }

    private ConfigVerificationResult.Outcome findFailedOutcome(final List<ConfigVerificationResult> results) {
        return results.stream()
                .map(ConfigVerificationResult::getOutcome)
                .filter(Predicate.isEqual(ConfigVerificationResult.Outcome.FAILED))
                .findFirst()
                .orElse(null);
    }

    private enum HttpMethod {
        GET,

        PUT,

        POST
    }

    private static ChannelStatus getChannelStatus(final String lastCommittedOffsetToken, final String channelStatusCode) {
        return new ChannelStatus(
                channelStatusCode,
                lastCommittedOffsetToken,
                System.currentTimeMillis(),
                DATABASE,
                SCHEMA,
                PIPE,
                FIRST_CHANNEL_NAME,
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
    }

    private static ChannelStatus getChannelStatusErrorCount() {
        return new ChannelStatus(
                ChannelStatusCode.SUCCESS.getStatus(),
                FIRST_OFFSET_TOKEN,
                System.currentTimeMillis(),
                DATABASE,
                SCHEMA,
                PIPE,
                FIRST_CHANNEL_NAME,
                0,
                0,
                ROWS_ERROR_COUNT,
                0,
                null,
                0,
                null,
                PARSING_FAILED_MESSAGE,
                0
        );
    }

    private static String getPipeKey() {
        final SecureRandom secureRandom = new SecureRandom();
        final byte[] randomKey = new byte[PIPE_KEY_LENGTH];
        secureRandom.nextBytes(randomKey);
        return encoder.encodeToString(randomKey);
    }

    private static RowsetStageLocation getStageLocation(final String location) {
        final Map<String, String> credentials = Map.of(
                S3CredentialProperty.AWS_KEY_ID.name(), S3CredentialProperty.AWS_KEY_ID.name(),
                S3CredentialProperty.AWS_SECRET_KEY.name(), S3CredentialProperty.AWS_SECRET_KEY.name(),
                S3CredentialProperty.AWS_TOKEN.name(), S3CredentialProperty.AWS_TOKEN.name()
        );

        return new RowsetStageLocation(null, 0, LocationType.S3.name(), false, location, STAGE_LOCATION_BUCKET, "", STAGE_LOCATION_REGION, null, null, null, credentials);
    }

    private class LocalStreamingClientProvider extends StandardStreamingClientProvider {
        @Override
        public URI getAccountUri(final ProcessContext context) {
            return accountUri;
        }

        @Override
        public URI getStreamingUri(final ProcessContext context) {
            final URI streamingUri = super.getStreamingUri(context);
            return URI.create(STREAMING_HOSTNAME_FORMAT.formatted(streamingUri.getHost(), accountUri.getPort()));
        }
    }

    private class LocalStandardWebServiceClient extends StandardWebClientService {
        @Override
        public HttpRequestUriSpec put() {
            final HttpRequestUriSpec httpRequestUriSpec = super.put();
            return new LocalHttpRequestUriSpec(httpRequestUriSpec);
        }
    }

    private class LocalHttpRequestUriSpec implements HttpRequestUriSpec {
        private final HttpRequestUriSpec httpRequestUriSpec;

        private LocalHttpRequestUriSpec(final HttpRequestUriSpec httpRequestUriSpec) {
            this.httpRequestUriSpec = httpRequestUriSpec;
        }

        @Override
        public HttpRequestBodySpec uri(final URI uri) {
            final URI requestUri;

            final String host = uri.getHost();
            if (S3_HOST.equals(host)) {
                requestUri = accountUri.resolve(uri.getPath());
            } else {
                requestUri = uri;
            }
            return httpRequestUriSpec.uri(requestUri);
        }
    }
}
