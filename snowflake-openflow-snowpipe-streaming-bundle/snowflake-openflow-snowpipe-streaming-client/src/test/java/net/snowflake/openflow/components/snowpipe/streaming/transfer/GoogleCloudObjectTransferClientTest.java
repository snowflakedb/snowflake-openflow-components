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

import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.MediaType;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.LocationType;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.PipeEncryptionInfo;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.RowsetStageLocation;
import net.snowflake.openflow.components.snowpipe.streaming.security.BufferEncryptor;
import net.snowflake.openflow.components.snowpipe.streaming.security.EncryptedBuffer;
import net.snowflake.openflow.components.snowpipe.streaming.security.EncryptionVersion;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GoogleCloudObjectTransferClientTest {
    private static final String NDJSON_ROW = "{\"ID\":1}\n";

    private static final int ROW_COUNT = 1;

    private static final String OFFSET_TOKEN = "1000.1";

    private static final String STAGE_LOCATION_BUCKET = "staging";

    private static final String STAGE_LOCATION_PATH = "/holding/location/";

    private static final String STAGE_LOCATION_FORMAT = "%s%s";

    private static final String STAGE_LOCATION_REGION = "region";

    private static final Pattern FILE_PATH_PATTERN = Pattern.compile("staging/holding/location/[a-z0-9-]{36}.ndjson");

    private static final String STANDARD_STORAGE_HOST = "storage.googleapis.com";

    private static final String URL_OVERRIDE_HOST = "staging.storage.googleapis.com";

    private static final Pattern DEFAULT_URI_PATH_PATTERN = Pattern.compile("/staging/holding/location/[a-z0-9-]{36}.ndjson");

    private static final Pattern VIRTUAL_HOSTED_URI_PATH_PATTERN = Pattern.compile("/holding/location/[a-z0-9-]{36}.ndjson");

    private static final String GCS_ACCESS_TOKEN = "access-token";

    private static final Map<String, String> CREDENTIALS = Map.of(
            GoogleCloudCredentialProperty.GCS_ACCESS_TOKEN.getProperty(), GCS_ACCESS_TOKEN
    );

    private static final RowsetStageLocation STAGE_LOCATION = new RowsetStageLocation(
            null,
            0,
            LocationType.GCS.name(),
            false,
            STAGE_LOCATION_FORMAT.formatted(STAGE_LOCATION_BUCKET, STAGE_LOCATION_PATH),
            STAGE_LOCATION_BUCKET,
            STAGE_LOCATION_PATH,
            STAGE_LOCATION_REGION,
            null,
            null,
            null,
            CREDENTIALS
    );

    private static final RowsetStageLocation STAGE_LOCATION_URL_OVERRIDE = new RowsetStageLocation(
            null,
            0,
            LocationType.GCS.name(),
            false,
            STAGE_LOCATION_FORMAT.formatted(STAGE_LOCATION_BUCKET, STAGE_LOCATION_PATH),
            STAGE_LOCATION_BUCKET,
            STAGE_LOCATION_PATH,
            STAGE_LOCATION_REGION,
            null,
            null,
            URL_OVERRIDE_HOST,
            CREDENTIALS
    );

    private static final EncryptionVersion ENCRYPTION_VERSION = EncryptionVersion.AES_CTR_128_V1;

    private static final String IV = "1234567890";

    private static final String DIVERSIFIER = "01";

    private static final String PIPE_KEY_ID = "1";

    private static final String ENTITY_TAG = "d3df3beb9e37db";

    private static final String ENTITY_TAG_HEADER = "\"%s\"".formatted(ENTITY_TAG);

    private static final String ERROR_RESPONSE_BODY = "<Error><Code>InternalError</Code></Error>";

    @Mock
    private WebClientService webClientService;

    @Mock
    private HttpRequestUriSpec requestUriSpec;

    @Mock
    private HttpRequestBodySpec requestBodySpec;

    @Mock
    private HttpResponseEntity responseEntity;

    @Mock
    private HttpEntityHeaders responseHeaders;

    @Mock
    private BufferEncryptor bufferEncryptor;

    @Mock
    private PipeEncryptionInfo pipeEncryptionInfo;

    @Captor
    private ArgumentCaptor<URI> uriCaptor;

    private GoogleCloudObjectTransferClient objectTransferClient;

    @BeforeEach
    void setObjectTransferClient() {
        objectTransferClient = new GoogleCloudObjectTransferClient(webClientService, bufferEncryptor);
    }

    @Test
    void testPutFileFragment() {
        final ObjectTransferLocation objectTransferLocation = new ObjectTransferLocation(STAGE_LOCATION, pipeEncryptionInfo);

        assertFileFragmentTransferred(objectTransferLocation);

        verifyRequestUri(STANDARD_STORAGE_HOST, DEFAULT_URI_PATH_PATTERN);
    }

    @Test
    void testPutFileFragmentUrlOverride() {
        final ObjectTransferLocation objectTransferLocation = new ObjectTransferLocation(STAGE_LOCATION_URL_OVERRIDE, pipeEncryptionInfo);

        assertFileFragmentTransferred(objectTransferLocation);

        verifyRequestUri(URL_OVERRIDE_HOST, VIRTUAL_HOSTED_URI_PATH_PATTERN);
    }

    @Test
    void testPutFileFragmentMissingEntityTagHeader() {
        final ObjectTransferLocation objectTransferLocation = new ObjectTransferLocation(STAGE_LOCATION, pipeEncryptionInfo);

        final byte[] fileFragment = NDJSON_ROW.getBytes(StandardCharsets.UTF_8);
        final long uncompressedLengthBytes = fileFragment.length;
        setEncryptedBuffer(fileFragment);

        setRequestSpecs();
        when(responseEntity.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
        when(responseEntity.headers()).thenReturn(responseHeaders);
        when(responseHeaders.getFirstHeader(eq(HttpHeaderName.ETAG.getHeaderName()))).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, () ->
                objectTransferClient.putFileFragment(fileFragment, ROW_COUNT, uncompressedLengthBytes, objectTransferLocation, OFFSET_TOKEN)
        );
    }

    @Test
    void testPutFileFragmentServerError() {
        final ObjectTransferLocation objectTransferLocation = new ObjectTransferLocation(STAGE_LOCATION, pipeEncryptionInfo);

        final byte[] fileFragment = NDJSON_ROW.getBytes(StandardCharsets.UTF_8);
        final long uncompressedLengthBytes = fileFragment.length;
        setEncryptedBuffer(fileFragment);

        final int statusCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
        setRequestSpecs();
        when(responseEntity.statusCode()).thenReturn(statusCode);
        when(responseEntity.body()).thenReturn(new ByteArrayInputStream(ERROR_RESPONSE_BODY.getBytes(StandardCharsets.UTF_8)));

        final HttpResponseException exception = assertThrows(HttpResponseException.class, () ->
                objectTransferClient.putFileFragment(fileFragment, ROW_COUNT, uncompressedLengthBytes, objectTransferLocation, OFFSET_TOKEN)
        );

        assertEquals(statusCode, exception.getStatusCode());
        assertTrue(exception.getMessage().contains("PUT"), "Exception message should reference PUT method");
        assertTrue(exception.getMessage().contains(ERROR_RESPONSE_BODY), "Exception message should include response body");
    }

    private EncryptedBuffer setEncryptedBuffer(final byte[] fileFragment) {
        final byte[] buffer = Base64.getEncoder().encode(fileFragment);
        final EncryptedBuffer encryptedBuffer = new EncryptedBuffer(buffer, IV, DIVERSIFIER, PIPE_KEY_ID, ENCRYPTION_VERSION.getVersion());
        when(bufferEncryptor.encryptBuffer(eq(fileFragment), eq(pipeEncryptionInfo))).thenReturn(encryptedBuffer);
        return encryptedBuffer;
    }

    private void setRequestSpecs() {
        when(webClientService.put()).thenReturn(requestUriSpec);
        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(InputStream.class), any(OptionalLong.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.header(any(), any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseEntity);
    }

    private void setSuccessResponseEntity() {
        setRequestSpecs();
        when(responseEntity.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
        when(responseEntity.headers()).thenReturn(responseHeaders);
        when(responseHeaders.getFirstHeader(eq(HttpHeaderName.ETAG.getHeaderName()))).thenReturn(Optional.of(ENTITY_TAG_HEADER));
    }

    private void verifyRequestUri(final String hostExpected, final Pattern pathPattern) {
        verify(requestUriSpec).uri(uriCaptor.capture());
        final URI requestUri = uriCaptor.getValue();
        assertEquals(hostExpected, requestUri.getHost());
        final String path = requestUri.getPath();
        final Matcher pathMatcher = pathPattern.matcher(path);
        assertTrue(pathMatcher.matches(), "URI path [%s] does not match pattern [%s]".formatted(path, pathPattern));
    }

    private void assertFileFragmentTransferred(final ObjectTransferLocation objectTransferLocation) {
        final byte[] fileFragment = NDJSON_ROW.getBytes(StandardCharsets.UTF_8);
        final long uncompressedLengthBytes = fileFragment.length;
        final EncryptedBuffer encryptedBuffer = setEncryptedBuffer(fileFragment);
        setSuccessResponseEntity();

        final FileFragmentInfo fileFragmentInfo = objectTransferClient.putFileFragment(fileFragment, ROW_COUNT, uncompressedLengthBytes, objectTransferLocation, OFFSET_TOKEN);
        assertFileFragmentInfoEquals(fileFragmentInfo, encryptedBuffer, fileFragment);
    }

    private void assertFileFragmentInfoEquals(final FileFragmentInfo fileFragmentInfo, final EncryptedBuffer encryptedBuffer, final byte[] fileFragment) {
        assertNotNull(fileFragmentInfo);

        final String filePath = fileFragmentInfo.filePath();
        final Matcher filePathMatcher = FILE_PATH_PATTERN.matcher(filePath);
        assertTrue(filePathMatcher.matches(), "File Path [%s] does not match pattern [%s]".formatted(filePath, FILE_PATH_PATTERN));

        assertEquals(MediaType.APPLICATION_NDJSON.getMediaType(), fileFragmentInfo.contentType());
        assertEquals(ENTITY_TAG, fileFragmentInfo.fileEtag());

        final byte[] buffer = encryptedBuffer.buffer();
        assertEquals(buffer.length, fileFragmentInfo.fragmentLengthBytes());
        assertEquals(buffer.length, fileFragmentInfo.fileLengthBytes());
        assertEquals(fileFragment.length, fileFragmentInfo.fragmentLengthUncompressedBytes());
        assertEquals(OFFSET_TOKEN, fileFragmentInfo.fragmentEndOffsetToken());
        assertEquals(0, fileFragmentInfo.fragmentStartByteOffset());
        assertEquals(1, fileFragmentInfo.fragmentRowCount());

        assertEquals(DIVERSIFIER, fileFragmentInfo.diversifier());
        assertEquals(PIPE_KEY_ID, fileFragmentInfo.masterKeyId());
        assertEquals(IV, fileFragmentInfo.ivBase64());
        assertEquals(ENCRYPTION_VERSION.getVersion(), fileFragmentInfo.encryptionVersion());
    }
}
