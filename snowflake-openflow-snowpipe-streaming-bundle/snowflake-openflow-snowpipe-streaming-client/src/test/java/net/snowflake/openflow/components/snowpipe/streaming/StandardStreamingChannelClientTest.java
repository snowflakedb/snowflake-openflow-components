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

package net.snowflake.openflow.components.snowpipe.streaming;

import net.snowflake.openflow.components.snowpipe.streaming.authorization.RequestAuthorization;
import net.snowflake.openflow.components.snowpipe.streaming.authorization.RequestAuthorizationProvider;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.BulkChannelNames;
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
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestHeadersSpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardStreamingChannelClientTest {

    private static final URI STREAMING_URI = URI.create("https://localhost");

    private static final String DATABASE = "OPENFLOW";
    private static final String SCHEMA = "PUBLIC";
    private static final String PIPE = "PIPE SPACED";
    private static final String CHANNEL = "SHARED";
    private static final String ELASTIC_CHANNEL = "ELASTIC";

    private static final String TABLE_DEFAULT_PIPE = "\"Unreserved0-9._~Plus!$&'()*+,;=:@Delimiters Table-STREAMING\"";
    private static final String TABLE_DEFAULT_PIPE_PATH = "/pipes/%22Unreserved0-9._~Plus!$&'()*+,%3B=:@Delimiters%20Table-STREAMING%22/channels/SHARED";

    private static final String AUTHORIZATION = "TOKEN";
    private static final String NEXT_CONTINUATION_TOKEN = "0_1";
    private static final String INSERT_CONTINUATION_TOKEN = "0_2";
    private static final String OFFSET_TOKEN = "1";

    private static final String OPEN_CHANNEL_REQUEST_BODY = "{\"fail_on_uncommitted_rows\":false}";
    private static final byte[] EMPTY_ROWS = {};

    private static final UUID REQUEST_ID = UUID.randomUUID();

    private static final String REQUEST_ID_QUERY = "requestId=%s".formatted(REQUEST_ID);
    private static final String FILE_FRAGMENT_URI_PATH = "/PIPE%20SPACED/channels/SHARED/filebuffer";
    private static final String ELASTIC_CHANNEL_FILE_FRAGMENT_URI_PATH = "/PIPE%20SPACED/channels/ELASTIC/filebuffer";
    private static final String ROWS_URI_PATH = "/PIPE%20SPACED/channels/SHARED/rows";
    private static final String ELASTIC_CHANNEL_ROWS_URI_PATH = "/PIPE%20SPACED/channels/ELASTIC/rows";
    private static final String CHANNEL_STATUS_PATH = "/pipes/PIPE%20SPACED:bulk-channel-status";
    private static final String PIPE_INFO_PATH = "/pipes/PIPE%20SPACED:pipe-info";

    private static final String ROWSET_STAGE_LOCATION = "staging/rowset/";
    private static final String DURABLE_STAGE_LOCATION = "staging/durable/";

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    @Mock
    private WebClientService webClientService;

    @Mock
    private RequestAuthorizationProvider requestAuthorizationProvider;

    @Mock
    private HttpRequestUriSpec requestUriSpec;

    @Mock
    private HttpRequestBodySpec requestBodySpec;

    @Mock
    private HttpRequestHeadersSpec requestHeadersSpec;

    @Captor
    private ArgumentCaptor<URI> uriCaptor;

    @Captor
    private ArgumentCaptor<String> headerValueCaptor;

    private StandardStreamingChannelClient client;

    @BeforeEach
    void setClient() {
        client = new StandardStreamingChannelClient(webClientService, requestAuthorizationProvider, STREAMING_URI);
    }

    @Test
    void testGetBulkChannelStatus() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any())).thenReturn(requestHeadersSpec);
        setStandardHeaders();

        final BulkChannelStatus expectedBulkChannelStatus = new BulkChannelStatus(Map.of());
        setResponseBody(expectedBulkChannelStatus);

        final BulkChannelNames bulkChannelNames = new BulkChannelNames(List.of(CHANNEL));
        final BulkChannelStatus bulkChannelStatus = client.getBulkChannelStatus(DATABASE, SCHEMA, PIPE, bulkChannelNames, REQUEST_ID);

        assertEquals(expectedBulkChannelStatus, bulkChannelStatus);

        verifyRequestUri(CHANNEL_STATUS_PATH);
    }

    @Test
    void testGetPipeInfo() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any())).thenReturn(requestHeadersSpec);
        setStandardHeaders();

        final PipeInfo expectedPipeInfo = new PipeInfo(
                ChannelStatusCode.SUCCESS.ordinal(),
                ChannelStatusCode.SUCCESS.getStatus(),
                getStageLocation(ROWSET_STAGE_LOCATION),
                mock(PipeEncryptionInfo.class),
                null
        );
        setResponseBody(expectedPipeInfo);

        final PipeInfo pipeInfo = client.getPipeInfo(DATABASE, SCHEMA, PIPE, REQUEST_ID);
        assertEquals(expectedPipeInfo.message(), pipeInfo.message());
        assertEquals(ROWSET_STAGE_LOCATION, pipeInfo.rowsetStageLocation().location());

        assertNull(pipeInfo.durableStageLocation());

        verifyRequestUri(PIPE_INFO_PATH);
    }

    @Test
    void testGetPipeInfoDurableStageLocationProvided() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any())).thenReturn(requestHeadersSpec);
        setStandardHeaders();

        final PipeInfo expectedPipeInfo = new PipeInfo(
                ChannelStatusCode.SUCCESS.ordinal(),
                ChannelStatusCode.SUCCESS.getStatus(),
                getStageLocation(ROWSET_STAGE_LOCATION),
                mock(PipeEncryptionInfo.class),
                getStageLocation(DURABLE_STAGE_LOCATION)
        );
        setResponseBody(expectedPipeInfo);

        final PipeInfo pipeInfo = client.getPipeInfo(DATABASE, SCHEMA, PIPE, REQUEST_ID);
        assertEquals(ROWSET_STAGE_LOCATION, pipeInfo.rowsetStageLocation().location());
        assertEquals(DURABLE_STAGE_LOCATION, pipeInfo.durableStageLocation().location());
    }

    private RowsetStageLocation getStageLocation(final String location) {
        return new RowsetStageLocation(
                null,
                0,
                LocationType.S3.name(),
                false,
                location,
                null,
                null,
                null,
                null,
                null,
                null,
                Map.of()
        );
    }

    @Test
    void testOpenChannel() {
        when(webClientService.put()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(eq(OPEN_CHANNEL_REQUEST_BODY))).thenReturn(requestHeadersSpec);
        setStandardHeaders();

        final ChannelStatus channelStatus = mock(ChannelStatus.class);
        final OpenedChannel expectedOpenedChannel = new OpenedChannel(NEXT_CONTINUATION_TOKEN, channelStatus);
        setResponseBody(expectedOpenedChannel);

        final OpenedChannel openedChannel = client.openChannel(DATABASE, SCHEMA, PIPE, CHANNEL, REQUEST_ID);

        assertNotNull(openedChannel);
        assertEquals(NEXT_CONTINUATION_TOKEN, openedChannel.nextContinuationToken());
    }

    @Test
    void testOpenChannelPathSegmentEncoding() {
        when(webClientService.put()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(eq(OPEN_CHANNEL_REQUEST_BODY))).thenReturn(requestHeadersSpec);
        setStandardHeaders();

        final ChannelStatus channelStatus = mock(ChannelStatus.class);
        final OpenedChannel expectedOpenedChannel = new OpenedChannel(NEXT_CONTINUATION_TOKEN, channelStatus);
        setResponseBody(expectedOpenedChannel);

        final OpenedChannel openedChannel = client.openChannel(DATABASE, SCHEMA, TABLE_DEFAULT_PIPE, CHANNEL, REQUEST_ID);

        assertNotNull(openedChannel);
        assertEquals(NEXT_CONTINUATION_TOKEN, openedChannel.nextContinuationToken());

        verifyRequestUri(TABLE_DEFAULT_PIPE_PATH);
    }

    @Test
    void testInsertFileFragments() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        setStandardHeaders();
        final List<FileFragmentInfo> fileFragmentInfos = setFileFragmentInfoRequestBody();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, INSERT_CONTINUATION_TOKEN);
        setInsertStatusResponseBody(expectedInsertStatus);

        final InsertStatus insertStatus = client.insertFileFragments(DATABASE, SCHEMA, PIPE, CHANNEL, NEXT_CONTINUATION_TOKEN, fileFragmentInfos, REQUEST_ID);
        assertEquals(expectedInsertStatus, insertStatus);

        verifyRequestUri(FILE_FRAGMENT_URI_PATH);
    }

    @Test
    void testInsertFileFragmentsElasticChannel() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        setStandardHeaders();
        final List<FileFragmentInfo> fileFragmentInfos = setFileFragmentInfoRequestBody();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, null);
        setInsertStatusResponseBody(expectedInsertStatus);

        final InsertStatus insertStatus = client.insertFileFragments(DATABASE, SCHEMA, PIPE, ELASTIC_CHANNEL, null, fileFragmentInfos, REQUEST_ID);
        assertEquals(expectedInsertStatus, insertStatus);

        verifyRequestUri(ELASTIC_CHANNEL_FILE_FRAGMENT_URI_PATH, REQUEST_ID_QUERY);
    }

    @Test
    void testInsertFileFragmentsSnowflakeSession() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        setStandardHeaders();
        final List<FileFragmentInfo> fileFragmentInfos = setFileFragmentInfoRequestBody();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, INSERT_CONTINUATION_TOKEN);
        final HttpEntityHeaders responseHeaders = setInsertStatusResponseBody(expectedInsertStatus);

        final String snowflakeSession = UUID.randomUUID().toString();
        setSnowflakeSession(responseHeaders, snowflakeSession);

        final InsertStatus insertStatus = client.insertFileFragments(DATABASE, SCHEMA, PIPE, CHANNEL, NEXT_CONTINUATION_TOKEN, fileFragmentInfos, REQUEST_ID);
        assertEquals(expectedInsertStatus, insertStatus);

        final ArgumentCaptor<String> snowflakeSessionCaptor = ArgumentCaptor.forClass(String.class);
        when(requestBodySpec.header(eq(StreamingHeader.SNOWFLAKE_SESSION.getHeaderName()), snowflakeSessionCaptor.capture())).thenReturn(requestBodySpec);
        setInsertStatusResponseBody(expectedInsertStatus);

        final String nextContinuationToken = insertStatus.nextContinuationToken();
        final InsertStatus secondInsertStatus = client.insertFileFragments(DATABASE, SCHEMA, PIPE, CHANNEL, nextContinuationToken, fileFragmentInfos, REQUEST_ID);
        assertEquals(expectedInsertStatus, secondInsertStatus);

        assertEquals(snowflakeSession, snowflakeSessionCaptor.getValue());
    }

    @Test
    void testInsertRows() {
        setInsertRowsRequest();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, INSERT_CONTINUATION_TOKEN);
        setInsertStatusResponseBody(expectedInsertStatus);

        final InsertStatus insertStatus = client.insertRows(DATABASE, SCHEMA, PIPE, CHANNEL, NEXT_CONTINUATION_TOKEN, OFFSET_TOKEN, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(expectedInsertStatus, insertStatus);
        verifyRequestUri(ROWS_URI_PATH);

        verifyUserAgentFound();
    }

    @Test
    void testInsertRowsUserAgentTags() {
        final String userAgentTags = StandardStreamingChannelClientTest.class.getSimpleName();
        client = new StandardStreamingChannelClient(webClientService, requestAuthorizationProvider, STREAMING_URI, userAgentTags);

        setInsertRowsRequest();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, INSERT_CONTINUATION_TOKEN);
        setInsertStatusResponseBody(expectedInsertStatus);

        final InsertStatus insertStatus = client.insertRows(DATABASE, SCHEMA, PIPE, CHANNEL, NEXT_CONTINUATION_TOKEN, OFFSET_TOKEN, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(expectedInsertStatus, insertStatus);
        verifyRequestUri(ROWS_URI_PATH);

        verify(requestBodySpec).header(eq(HttpHeaderName.USER_AGENT.getHeaderName()), headerValueCaptor.capture());
        final String userAgent = headerValueCaptor.getValue();
        assertTrue(userAgent.startsWith(UserAgentProvider.getUserAgent()), "User-Agent [%s] platform information not found".formatted(userAgent));
        assertTrue(userAgent.contains(userAgentTags), "Tags [%s] not found in User-Agent [%s]".formatted(userAgentTags, userAgent));
    }

    @Test
    void testInsertRowsElasticChannel() {
        setInsertRowsRequest();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, null);
        setInsertStatusResponseBody(expectedInsertStatus);

        final InsertStatus insertStatus = client.insertRows(DATABASE, SCHEMA, PIPE, ELASTIC_CHANNEL, null, null, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(expectedInsertStatus, insertStatus);
        verifyRequestUri(ELASTIC_CHANNEL_ROWS_URI_PATH, REQUEST_ID_QUERY);
    }

    @Test
    void testInsertRowsSnowflakeSessionCleared() {
        setInsertRowsRequest();

        final InsertStatus expectedInsertStatus = new InsertStatus(0, null, INSERT_CONTINUATION_TOKEN);
        final String snowflakeSession = UUID.randomUUID().toString();

        final HttpEntityHeaders firstResponseHeaders = setInsertStatusResponseBody(expectedInsertStatus);
        setSnowflakeSession(firstResponseHeaders, snowflakeSession);

        final InsertStatus firstInsertStatus = client.insertRows(DATABASE, SCHEMA, PIPE, CHANNEL, NEXT_CONTINUATION_TOKEN, OFFSET_TOKEN, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(expectedInsertStatus, firstInsertStatus);

        final ArgumentCaptor<String> snowflakeSessionCaptor = ArgumentCaptor.forClass(String.class);
        when(requestBodySpec.header(eq(StreamingHeader.SNOWFLAKE_SESSION.getHeaderName()), snowflakeSessionCaptor.capture())).thenReturn(requestBodySpec);

        final HttpEntityHeaders secondResponseHeaders = setInsertStatusResponseBody(expectedInsertStatus);
        setSnowflakeSession(secondResponseHeaders, null);

        final InsertStatus secondInsertStatus = client.insertRows(DATABASE, SCHEMA, PIPE, CHANNEL, firstInsertStatus.nextContinuationToken(), OFFSET_TOKEN, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(expectedInsertStatus, secondInsertStatus);
        assertEquals(snowflakeSession, snowflakeSessionCaptor.getValue());

        final String restoredSession = UUID.randomUUID().toString();
        final HttpEntityHeaders thirdResponseHeaders = setInsertStatusResponseBody(expectedInsertStatus);
        setSnowflakeSession(thirdResponseHeaders, restoredSession);

        final InsertStatus thirdInsertStatus = client.insertRows(DATABASE, SCHEMA, PIPE, CHANNEL, secondInsertStatus.nextContinuationToken(), OFFSET_TOKEN, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(expectedInsertStatus, thirdInsertStatus);
        assertEquals(1, snowflakeSessionCaptor.getAllValues().size());

        setInsertStatusResponseBody(expectedInsertStatus);

        client.insertRows(DATABASE, SCHEMA, PIPE, CHANNEL, thirdInsertStatus.nextContinuationToken(), OFFSET_TOKEN, EMPTY_ROWS, 0, REQUEST_ID);
        assertEquals(restoredSession, snowflakeSessionCaptor.getAllValues().get(1));
    }

    private void verifyRequestUri(final String pathExpected, final String queryExpected) {
        final URI requestUri = verifyRequestUri(pathExpected);
        final String query = requestUri.getQuery();
        assertEquals(queryExpected, query);
    }

    private URI verifyRequestUri(final String pathExpected) {
        verify(requestUriSpec).uri(uriCaptor.capture());

        final URI requestUri = uriCaptor.getValue();
        final String path = requestUri.getRawPath();
        final String query = requestUri.getRawQuery();

        assertTrue(path.endsWith(pathExpected), "Request URI Path [%s] missing [%s]".formatted(path, pathExpected));
        assertTrue(query.startsWith(REQUEST_ID_QUERY), "Request URI Query [%s] missing [%s]".formatted(query, REQUEST_ID_QUERY));

        return requestUri;
    }

    private void verifyUserAgentFound() {
        verify(requestBodySpec).header(eq(HttpHeaderName.USER_AGENT.getHeaderName()), headerValueCaptor.capture());
        final String userAgent = headerValueCaptor.getValue();
        assertEquals(UserAgentProvider.getUserAgent(), userAgent);
    }

    private void setInsertRowsRequest() {
        when(webClientService.post()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(), any())).thenReturn(requestHeadersSpec);
        setStandardHeaders();
    }

    private void setStandardHeaders() {
        final RequestAuthorization requestAuthorization = new RequestAuthorization(AUTHORIZATION, ZonedDateTime.now());
        when(requestAuthorizationProvider.getRequestAuthorization()).thenReturn(requestAuthorization);

        when(requestBodySpec.header(anyString(), anyString())).thenReturn(requestBodySpec);
        lenient().when(requestHeadersSpec.header(anyString(), anyString())).thenReturn(requestBodySpec);
    }

    private List<FileFragmentInfo> setFileFragmentInfoRequestBody() {
        final FileFragmentInfo fileFragmentInfo = mock(FileFragmentInfo.class);
        final List<FileFragmentInfo> fileFragmentInfos = List.of(fileFragmentInfo);
        final String serializedFileFragmentInfos = MAPPER.writeValueAsString(fileFragmentInfos);
        when(requestBodySpec.body(eq(serializedFileFragmentInfos))).thenReturn(requestBodySpec);
        when(requestBodySpec.header(eq(HttpHeaderName.CONTENT_TYPE.getHeaderName()), eq(MediaType.APPLICATION_JSON.getMediaType()))).thenReturn(requestBodySpec);
        return fileFragmentInfos;
    }

    private void setResponseBody(final Object responseObject) {
        final HttpResponseEntity responseEntity = mock(HttpResponseEntity.class);
        when(requestBodySpec.retrieve()).thenReturn(responseEntity);

        final byte[] serializedResponseObject = MAPPER.writeValueAsBytes(responseObject);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedResponseObject);
        when(responseEntity.body()).thenReturn(inputStream);
        when(responseEntity.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
    }

    private HttpEntityHeaders setInsertStatusResponseBody(final InsertStatus insertStatus) {
        final HttpResponseEntity responseEntity = mock(HttpResponseEntity.class);
        when(requestBodySpec.retrieve()).thenReturn(responseEntity);

        final byte[] serializedResponseObject = MAPPER.writeValueAsBytes(insertStatus);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(serializedResponseObject);
        when(responseEntity.body()).thenReturn(inputStream);
        when(responseEntity.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);

        final HttpEntityHeaders responseHeaders = mock(HttpEntityHeaders.class);
        when(responseEntity.headers()).thenReturn(responseHeaders);

        return responseHeaders;
    }

    private void setSnowflakeSession(final HttpEntityHeaders responseHeaders, final String session) {
        final Optional<String> sessionHeader = Optional.ofNullable(session);
        when(responseHeaders.getFirstHeader(StreamingHeader.SNOWFLAKE_SESSION.getHeaderName())).thenReturn(sessionHeader);
    }
}
