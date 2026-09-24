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
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Azure Object Transfer Client supporting Block Blob transmission for Azure Storage
 */
class AzureObjectTransferClient implements ObjectTransferClient {
    private static final String BLOCK_BLOB_TYPE = "BlockBlob";

    private static final String AZURE_URI_FORMAT = "https://%s.%s/%s";

    private static final String AZURE_SAS_URI_FORMAT = "%s%s";

    private static final String FILE_PATH_FORMAT = "%s%s.ndjson";

    private static final long START_BYTE_OFFSET = 0;

    private final WebClientService webClientService;

    private final BufferEncryptor bufferEncryptor;

    public AzureObjectTransferClient(final WebClientService webClientService, final BufferEncryptor bufferEncryptor) {
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

        final String sasToken = credentials.get(AzureCredentialProperty.AZURE_SAS_TOKEN.getProperty());
        if (sasToken == null) {
            throw new IllegalStateException("Azure SAS Token not found in Rowset Stage Location");
        }

        final String storageAccount = stageLocation.storageAccount();
        final String storageEndpoint = stageLocation.storageEndpoint();
        final String filePath = FILE_PATH_FORMAT.formatted(stageLocation.location(), UUID.randomUUID());
        // Blob URI excludes the SAS Token so that failures can be reported without disclosing credentials
        final URI blobUri = URI.create(AZURE_URI_FORMAT.formatted(storageAccount, storageEndpoint, filePath));
        final URI requestUri = URI.create(AZURE_SAS_URI_FORMAT.formatted(blobUri, sasToken));

        final EncryptedBuffer encryptedBuffer = getEncryptedBuffer(fileFragment, objectTransferLocation);
        final byte[] payload = encryptedBuffer.buffer();

        final String dateTime = DateTimeFormatter.RFC_1123_DATE_TIME.format(OffsetDateTime.now());
        final HttpRequestBodySpec requestBodySpec = webClientService.put()
                .uri(requestUri)
                .body(new ByteArrayInputStream(payload), OptionalLong.of(payload.length))
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_NDJSON.getMediaType())
                .header(AzureHttpHeader.MS_BLOB_TYPE.getHeader(), BLOCK_BLOB_TYPE)
                .header(AzureHttpHeader.MS_DATE.getHeader(), dateTime);

        final String entityTag;
        try (HttpResponseEntity httpResponseEntity = requestBodySpec.retrieve()) {
            final int statusCode = httpResponseEntity.statusCode();

            if (HttpURLConnection.HTTP_CREATED == statusCode) {
                final HttpEntityHeaders responseHeaders = httpResponseEntity.headers();
                entityTag = getEntityTag(responseHeaders);
            } else {
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                try (InputStream responseBody = httpResponseEntity.body()) {
                    responseBody.transferTo(outputStream);
                }
                final String responseBody = outputStream.toString(StandardCharsets.UTF_8);
                throw new HttpResponseException("Azure PUT Failed [%s]".formatted(responseBody), blobUri, StandardHttpRequestMethod.PUT.getMethod(), statusCode);
            }
        } catch (final IOException e) {
            throw new HttpClientException("Azure Request communication failed", e, blobUri, StandardHttpRequestMethod.PUT.getMethod());
        }

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

    private EncryptedBuffer getEncryptedBuffer(final byte[] fileFragment, final ObjectTransferLocation objectTransferLocation) {
        final PipeEncryptionInfo pipeEncryptionInfo = objectTransferLocation.pipeEncryptionInfo();
        return bufferEncryptor.encryptBuffer(fileFragment, pipeEncryptionInfo);
    }

    private String getEntityTag(final HttpEntityHeaders responseHeaders) {
        final Optional<String> entityTagHeader = responseHeaders.getFirstHeader(HttpHeaderName.ETAG.getHeaderName());
        final String entityTag = entityTagHeader.orElse(null);

        if (entityTag == null) {
            throw new IllegalStateException("Azure ETag header not found");
        }
        return entityTag;
    }
}
