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
import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.InsertStatus;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.OpenedChannel;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeInfo;
import net.snowflake.openflow.components.snowpipe.streaming.security.EncryptionVersion;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestMethod;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientService;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.PropertyNamingStrategies;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static net.snowflake.openflow.components.snowpipe.streaming.UriEncoder.getSegmentEncoded;

/**
 * Standard implementation of Snowpipe Streaming Channel Client using Web Client Service for requests
 */
public class StandardStreamingChannelClient implements StreamingChannelClient {
    /** Object Mapper configured with Snake Cake strategy for converting response payload body */
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private static final String REQUEST_ID_QUERY_PARAMETER = "?requestId=";

    private static final String ENCRYPTION_VERSION_PARAMETER = "&encryptionVersion=";

    private static final String CONTINUATION_TOKEN_PARAMETER = "&continuationToken=";

    private static final String OFFSET_TOKEN_PARAMETER = "&offsetToken=";

    private static final String PIPE_INFO_REQUEST_BODY = "{\"generate_new_token\":true}";

    /** Disable HTTP request failures when opening channels with rows being processed to require client replay */
    private static final String OPEN_CHANNEL_REQUEST_BODY = "{\"fail_on_uncommitted_rows\":false}";

    private static final String ZSTD_ENCODING = "zstd";

    private static final String DATA_CHANNEL_PATH_FORMAT = "/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s";

    private static final String FILE_FRAGMENT_URI_FORMAT = "%s/filebuffer";

    private static final String ROWS_URI_FORMAT = "%s/rows";

    private static final String USER_AGENT_FORMAT = "%s [%s]";

    private final ConcurrentMap<String, String> snowflakeSessions = new ConcurrentHashMap<>();

    private final WebClientService webClientService;

    private final RequestAuthorizationProvider requestAuthorizationProvider;

    private final URI streamingUri;

    private final String userAgent;

    /**
     * Standard Streaming Channel Client constructor with required properties
     *
     * @param webClientService Web Client Service required
     * @param requestAuthorizationProvider Request Authorization Provider required
     * @param streamingUri Streaming URI required
     */
    public StandardStreamingChannelClient(
            final WebClientService webClientService,
            final RequestAuthorizationProvider requestAuthorizationProvider,
            final URI streamingUri
    ) {
        this.webClientService = webClientService;
        this.requestAuthorizationProvider = requestAuthorizationProvider;
        this.streamingUri = streamingUri;
        this.userAgent = UserAgentProvider.getUserAgent();
    }

    /**
     * Standard Streaming Channel Client constructor with User-Agent Tags
     *
     * @param webClientService Web Client Service required
     * @param requestAuthorizationProvider Request Authorization Provider required
     * @param streamingUri Streaming URI required
     * @param userAgentTags User-Agent Tags required and appended to platform information
     */
    public StandardStreamingChannelClient(
            final WebClientService webClientService,
            final RequestAuthorizationProvider requestAuthorizationProvider,
            final URI streamingUri,
            final String userAgentTags
    ) {
        this.webClientService = webClientService;
        this.requestAuthorizationProvider = requestAuthorizationProvider;
        this.streamingUri = streamingUri;
        this.userAgent = USER_AGENT_FORMAT.formatted(UserAgentProvider.getUserAgent(), userAgentTags);
    }

    /**
     * Get Bulk Channel Status for specified Channel Names
     *
     * @param database Database Name
     * @param schema Schema Name
     * @param pipe Pipe Name
     * @param bulkChannelNames Channel Names to be checked
     * @param requestId Request Identifier for correlated tracking
     * @return Bulk Channel Status
     */
    @Override
    public BulkChannelStatus getBulkChannelStatus(final String database, final String schema, final String pipe, final BulkChannelNames bulkChannelNames, final UUID requestId) {
        final String path = "/v2/streaming/databases/%s/schemas/%s/pipes/%s:bulk-channel-status".formatted(
                getSegmentEncoded(database), getSegmentEncoded(schema), getSegmentEncoded(pipe)
        );
        final StringBuilder requestPathBuilder = getRequestPathBuilder(path, requestId);
        final URI uri = streamingUri.resolve(requestPathBuilder.toString());

        final String requestBody = MAPPER.writeValueAsString(bulkChannelNames);

        final HttpRequestBodySpec requestBodySpec = webClientService.post()
                .uri(uri)
                .body(requestBody)
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_JSON.getMediaType());
        return send(requestBodySpec, StandardHttpRequestMethod.POST, uri, BulkChannelStatus.class);
    }

    @Override
    public PipeInfo getPipeInfo(final String database, final String schema, final String pipe, final UUID requestId) {
        final String path = "/v2/streaming/databases/%s/schemas/%s/pipes/%s:pipe-info".formatted(
                getSegmentEncoded(database), getSegmentEncoded(schema), getSegmentEncoded(pipe)
        );
        final StringBuilder requestPathBuilder = getRequestPathBuilder(path, requestId);
        requestPathBuilder.append(ENCRYPTION_VERSION_PARAMETER);
        requestPathBuilder.append(EncryptionVersion.AES_CTR_128_V1.getVersion());
        final URI uri = streamingUri.resolve(requestPathBuilder.toString());

        final HttpRequestBodySpec requestBodySpec = webClientService.post()
                .uri(uri)
                .body(PIPE_INFO_REQUEST_BODY)
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_JSON.getMediaType());
        return send(requestBodySpec, StandardHttpRequestMethod.POST, uri, PipeInfo.class);
    }

    @Override
    public OpenedChannel openChannel(final String database, final String schema, final String pipe, final String channel, final UUID requestId) {
        final String path = "/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s".formatted(
                getSegmentEncoded(database), getSegmentEncoded(schema), getSegmentEncoded(pipe), getSegmentEncoded(channel)
        );
        final StringBuilder requestPathBuilder = getRequestPathBuilder(path, requestId);
        final URI uri = streamingUri.resolve(requestPathBuilder.toString());

        final HttpRequestBodySpec requestBodySpec = webClientService.put()
                .uri(uri)
                .body(OPEN_CHANNEL_REQUEST_BODY)
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_JSON.getMediaType());
        return send(requestBodySpec, StandardHttpRequestMethod.PUT, uri, OpenedChannel.class);
    }

    @Override
    public InsertStatus insertRows(
            final String database,
            final String schema,
            final String pipe,
            final String channel,
            final String continuationToken,
            final String offsetToken,
            final byte[] rows,
            final long uncompressedContentLength,
            final UUID requestId
    ) {
        final String channelPath = DATA_CHANNEL_PATH_FORMAT.formatted(
                getSegmentEncoded(database), getSegmentEncoded(schema), getSegmentEncoded(pipe), getSegmentEncoded(channel)
        );
        final String path = ROWS_URI_FORMAT.formatted(channelPath);
        final StringBuilder requestPathBuilder = getRequestPathBuilder(path, requestId);

        if (isNotEmpty(continuationToken)) {
            requestPathBuilder.append(CONTINUATION_TOKEN_PARAMETER);
            requestPathBuilder.append(continuationToken);
        }

        if (isNotEmpty(offsetToken)) {
            requestPathBuilder.append(OFFSET_TOKEN_PARAMETER);
            requestPathBuilder.append(offsetToken);
        }

        final URI uri = streamingUri.resolve(requestPathBuilder.toString());

        final HttpRequestBodySpec requestBodySpec = webClientService.post()
                .uri(uri)
                .body(new ByteArrayInputStream(rows), OptionalLong.of(rows.length))
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_NDJSON.getMediaType())
                .header(HttpHeaderName.CONTENT_ENCODING.getHeaderName(), ZSTD_ENCODING)
                .header(StreamingHeader.UNCOMPRESSED_CONTENT_LENGTH.getHeaderName(), Long.toString(uncompressedContentLength));
        return sendInsert(requestBodySpec, uri, channelPath);
    }

    @Override
    public InsertStatus insertFileFragments(
            final String database,
            final String schema,
            final String pipe,
            final String channel,
            final String continuationToken,
            final List<FileFragmentInfo> fileFragments,
            final UUID requestId
    ) {
        final String channelPath = DATA_CHANNEL_PATH_FORMAT.formatted(
                getSegmentEncoded(database), getSegmentEncoded(schema), getSegmentEncoded(pipe), getSegmentEncoded(channel)
        );
        final String path = FILE_FRAGMENT_URI_FORMAT.formatted(channelPath);
        final StringBuilder requestPathBuilder = getRequestPathBuilder(path, requestId);

        if (isNotEmpty(continuationToken)) {
            requestPathBuilder.append(CONTINUATION_TOKEN_PARAMETER);
            requestPathBuilder.append(continuationToken);
        }

        final URI uri = streamingUri.resolve(requestPathBuilder.toString());

        final String requestBody = MAPPER.writeValueAsString(fileFragments);

        final HttpRequestBodySpec requestBodySpec = webClientService.post()
                .uri(uri)
                .body(requestBody)
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_JSON.getMediaType());
        return sendInsert(requestBodySpec, uri, channelPath);
    }

    private <T> T send(final HttpRequestBodySpec requestBodySpec, final HttpRequestMethod httpMethod, final URI uri, final Class<T> responseClass) {
        try (
                HttpResponseEntity httpResponseEntity = setStandardHeaders(requestBodySpec).retrieve();
                InputStream responseBody = httpResponseEntity.body()
        ) {
            final int statusCode = httpResponseEntity.statusCode();
            return getResponseObject(responseBody, uri, httpMethod, statusCode, responseClass);
        } catch (final IOException e) {
            throw new HttpClientException("Read Response JSON failed", e, uri, httpMethod.getMethod());
        }
    }

    private InsertStatus sendInsert(final HttpRequestBodySpec requestBodySpec, final URI uri, final String channelPath) {
        final HttpRequestMethod httpMethod = StandardHttpRequestMethod.POST;
        final HttpRequestBodySpec preparedRequestBodySpec = setSnowflakeSession(requestBodySpec, channelPath);
        final HttpRequestBodySpec authorizedRequestBodySpec = setStandardHeaders(preparedRequestBodySpec);
        try (
                HttpResponseEntity httpResponseEntity = authorizedRequestBodySpec.retrieve();
                InputStream responseBody = httpResponseEntity.body()
        ) {
            final int statusCode = httpResponseEntity.statusCode();
            final HttpEntityHeaders responseHeaders = httpResponseEntity.headers();
            processSnowflakeSessionStatus(responseHeaders, channelPath);
            return getResponseObject(responseBody, uri, httpMethod, statusCode, InsertStatus.class);
        } catch (final IOException e) {
            throw new HttpClientException("Read Response JSON failed", e, uri, httpMethod.getMethod());
        }
    }

    private <T> T getResponseObject(
            final InputStream responseBody,
            final URI uri,
            final HttpRequestMethod httpMethod,
            final int statusCode,
            final Class<T> responseClass
    ) {
        if (HttpURLConnection.HTTP_OK == statusCode) {
            return MAPPER.readValue(responseBody, responseClass);
        } else {
            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try {
                responseBody.transferTo(outputStream);
            } catch (final IOException e) {
                throw new HttpClientException("Read Response Body failed", e, uri, httpMethod.getMethod());
            }
            final String responseMessage = outputStream.toString(StandardCharsets.UTF_8);
            throw new HttpResponseException("Response [%s]".formatted(responseMessage), uri, httpMethod.getMethod(), statusCode);
        }
    }

    private HttpRequestBodySpec setStandardHeaders(final HttpRequestBodySpec requestBodySpec) {
        final RequestAuthorization requestAuthorization = requestAuthorizationProvider.getRequestAuthorization();
        return requestBodySpec
                .header(HttpHeaderName.ACCEPT.getHeaderName(), MediaType.APPLICATION_JSON.getMediaType())
                .header(HttpHeaderName.AUTHORIZATION.getHeaderName(), requestAuthorization.authorization())
                .header(HttpHeaderName.USER_AGENT.getHeaderName(), userAgent);
    }

    private HttpRequestBodySpec setSnowflakeSession(final HttpRequestBodySpec requestBodySpec, final String channelPath) {
        final HttpRequestBodySpec preparedRequestBodySpec;

        final String snowflakeSession = snowflakeSessions.get(channelPath);
        if (snowflakeSession == null || snowflakeSession.isEmpty()) {
            preparedRequestBodySpec = requestBodySpec;
        } else {
            preparedRequestBodySpec = requestBodySpec.header(StreamingHeader.SNOWFLAKE_SESSION.getHeaderName(), snowflakeSession);
        }

        return preparedRequestBodySpec;
    }

    private void processSnowflakeSessionStatus(final HttpEntityHeaders responseHeaders, final String channelPath) {
        final Optional<String> snowflakeSessionFound = responseHeaders.getFirstHeader(StreamingHeader.SNOWFLAKE_SESSION.getHeaderName());
        final String currentSnowflakeSession = snowflakeSessionFound.orElse(null);

        if (currentSnowflakeSession == null || currentSnowflakeSession.isEmpty()) {
            // Remove Snowflake Session for Channel when HTTP response does not contain header
            snowflakeSessions.remove(channelPath);
        } else {
            snowflakeSessions.put(channelPath, currentSnowflakeSession);
        }
    }

    private StringBuilder getRequestPathBuilder(final String path, final UUID requestId) {
        final StringBuilder builder = new StringBuilder();
        builder.append(path);
        builder.append(REQUEST_ID_QUERY_PARAMETER);
        builder.append(requestId);
        return builder;
    }

    private boolean isNotEmpty(final String string) {
        return string != null && !string.isEmpty();
    }
}
