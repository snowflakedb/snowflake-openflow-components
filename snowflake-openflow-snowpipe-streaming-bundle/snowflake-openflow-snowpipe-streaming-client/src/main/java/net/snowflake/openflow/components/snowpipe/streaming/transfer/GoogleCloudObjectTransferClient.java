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

package net.snowflake.openflow.components.snowpipe.streaming.transfer;

import net.snowflake.openflow.components.snowpipe.streaming.HttpClientException;
import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.MediaType;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeEncryptionInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.RowsetStageLocation;
import net.snowflake.openflow.components.snowpipe.streaming.security.BufferEncryptor;
import net.snowflake.openflow.components.snowpipe.streaming.security.EncryptedBuffer;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Google Cloud Storage Object Transfer Client supporting fragment transmission for Google Cloud Storage
 */
class GoogleCloudObjectTransferClient implements ObjectTransferClient {
    private static final String HTTPS_SCHEME = "https";

    private static final String STANDARD_STORAGE_HOST = "storage.googleapis.com";

    private static final String BEARER_TOKEN_FORMAT = "Bearer %s";

    private static final long START_BYTE_OFFSET = 0;

    private static final Pattern BUCKET_PATH_PATTERN = Pattern.compile("^([^/]+?)/(.+)$");

    private static final int BUCKET_GROUP = 1;

    private static final int OBJECT_PATH_GROUP = 2;

    private static final String FILE_NAME_FORMAT = "%s.ndjson";

    private static final String PATH_FORMAT = "%s%s";

    private static final String DEFAULT_URI_PATH_FORMAT = "/%s/%s";

    private static final String VIRTUAL_HOSTED_URI_PATH_FORMAT = "/%s";

    private static final Pattern ENTITY_TAG_PATTERN = Pattern.compile("^\"(.+?)\"$");

    private static final int FIRST_GROUP = 1;

    private final WebClientService webClientService;

    private final BufferEncryptor bufferEncryptor;

    public GoogleCloudObjectTransferClient(final WebClientService webClientService, final BufferEncryptor bufferEncryptor) {
        this.webClientService = webClientService;
        this.bufferEncryptor = bufferEncryptor;
    }

    @Override
    public FileFragmentInfo putFileFragment(
            final byte[] fileFragment,
            final long fragmentRowCount,
            final long uncompressedLengthBytes,
            final ObjectTransferLocation objectTransferLocation,
            final String offsetToken
    ) {
        final RowsetStageLocation stageLocation = objectTransferLocation.stageLocation();
        final Map<String, String> credentials = stageLocation.creds();

        final String accessToken = credentials.get(GoogleCloudCredentialProperty.GCS_ACCESS_TOKEN.getProperty());
        if (accessToken == null) {
            throw new IllegalStateException("Google Cloud Access Token not found in Rowset Stage Location");
        }
        final String authorization = BEARER_TOKEN_FORMAT.formatted(accessToken);

        final String location = stageLocation.location();
        final String filename = FILE_NAME_FORMAT.formatted(UUID.randomUUID());

        // URL Override replaces Google Cloud Storage host address with a virtual-hosted host already containing the bucket name
        final String urlOverride = stageLocation.urlOverride();
        final URI uri = getUri(urlOverride, location, filename);

        final EncryptedBuffer encryptedBuffer = getEncryptedBuffer(fileFragment, objectTransferLocation);
        final byte[] payload = encryptedBuffer.buffer();

        final HttpRequestBodySpec requestBodySpec = webClientService.put()
                .uri(uri)
                .body(new ByteArrayInputStream(payload), OptionalLong.of(payload.length))
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_NDJSON.getMediaType())
                .header(HttpHeaderName.AUTHORIZATION.getHeaderName(), authorization);

        final String entityTag;
        try (HttpResponseEntity httpResponseEntity = requestBodySpec.retrieve()) {
            final int statusCode = httpResponseEntity.statusCode();

            if (HttpURLConnection.HTTP_OK == statusCode) {
                final HttpEntityHeaders responseHeaders = httpResponseEntity.headers();
                entityTag = getEntityTag(responseHeaders);
            } else {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                try (InputStream responseBody = httpResponseEntity.body()) {
                    responseBody.transferTo(outputStream);
                }
                final String responseBody = outputStream.toString(StandardCharsets.UTF_8);
                throw new HttpResponseException("Google Cloud Storage PUT Failed [%s]".formatted(responseBody), uri, StandardHttpRequestMethod.PUT.getMethod(), statusCode);
            }
        } catch (final IOException e) {
            throw new HttpClientException("Google Cloud Storage Request communication failed", e, uri, StandardHttpRequestMethod.PUT.getMethod());
        }

        // File Path for File Fragment based on stage location
        final String filePath = PATH_FORMAT.formatted(location, filename);
        final long fileLengthBytes = payload.length;
        return new FileFragmentInfo(
                filePath,
                MediaType.APPLICATION_NDJSON.getMediaType(),
                entityTag,
                fileLengthBytes,
                fileLengthBytes,
                uncompressedLengthBytes,
                offsetToken,
                fragmentRowCount,
                START_BYTE_OFFSET,
                encryptedBuffer.diversifier(),
                encryptedBuffer.pipeKeyId(),
                encryptedBuffer.iv(),
                encryptedBuffer.encryptionVersion()
        );
    }

    private URI getUri(final String urlOverride, final String location, final String filename) {
        final Matcher matcher = BUCKET_PATH_PATTERN.matcher(location);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Bucket Path pattern not matched in location [%s]".formatted(location));
        }

        final String bucket = matcher.group(BUCKET_GROUP);
        final String objectPath = matcher.group(OBJECT_PATH_GROUP);

        // Google Cloud Storage Object Name includes the path without the bucket name
        final String objectName = PATH_FORMAT.formatted(objectPath, filename);

        final String host;
        final String uriPath;
        if (urlOverride == null) {
            // Build path-based URI for XML API
            host = STANDARD_STORAGE_HOST;
            uriPath = DEFAULT_URI_PATH_FORMAT.formatted(bucket, objectName);
        } else {
            // Build virtual-hosted URI for XML API
            host = urlOverride;
            uriPath = VIRTUAL_HOSTED_URI_PATH_FORMAT.formatted(objectName);
        }

        try {
            return new URI(HTTPS_SCHEME, host, uriPath, null, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Google Cloud Storage URI construction failed for host [%s] path [%s]".formatted(host, uriPath), e);
        }
    }

    private EncryptedBuffer getEncryptedBuffer(final byte[] fileFragment, final ObjectTransferLocation objectTransferLocation) {
        final PipeEncryptionInfo pipeEncryptionInfo = objectTransferLocation.pipeEncryptionInfo();
        return bufferEncryptor.encryptBuffer(fileFragment, pipeEncryptionInfo);
    }

    private String getEntityTag(final HttpEntityHeaders responseHeaders) {
        final Optional<String> entityTagHeader = responseHeaders.getFirstHeader(HttpHeaderName.ETAG.getHeaderName());
        final String entityTag = entityTagHeader.map(ENTITY_TAG_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(FIRST_GROUP))
                .orElse(null);

        if (entityTag == null) {
            throw new IllegalStateException("Google Cloud Storage ETag header not found");
        }
        return entityTag;
    }
}
