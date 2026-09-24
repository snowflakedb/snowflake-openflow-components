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

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3ObjectTransferClientTest {
    private static final String NDJSON_ROW = "{\"ID\":1}\n";

    private static final int ROW_COUNT = 1;

    private static final String OFFSET_TOKEN = "1000.1";

    private static final String STAGE_LOCATION_BUCKET = "staging";

    private static final String STAGE_LOCATION_PATH = "/holding/location/";

    private static final String STAGE_LOCATION_FORMAT = "%s%s";

    private static final String STAGE_LOCATION_REGION = "region";

    private static final Pattern FILE_PATH_PATTERN = Pattern.compile("staging/holding/location/[a-z0-9-]{36}.ndjson");

    private static final String AWS_KEY_ID = "Key-Identifier";

    private static final String STORAGE_ENDPOINT = "localhost";

    private static final String BUCKET_STORAGE_ENDPOINT = "%s.%s".formatted(STAGE_LOCATION_BUCKET, STORAGE_ENDPOINT);

    private static final String STANDARD_ENDPOINT = "%s.s3.%s.amazonaws.com".formatted(STAGE_LOCATION_BUCKET, STAGE_LOCATION_REGION);

    private static final Map<String, String> CREDENTIALS = Map.of(
            S3CredentialProperty.AWS_KEY_ID.name(), AWS_KEY_ID,
            S3CredentialProperty.AWS_SECRET_KEY.name(), S3CredentialProperty.AWS_SECRET_KEY.name(),
            S3CredentialProperty.AWS_TOKEN.name(), S3CredentialProperty.AWS_TOKEN.name()
    );

    private static final RowsetStageLocation STAGE_LOCATION = new RowsetStageLocation(
            null,
            0,
            LocationType.S3.name(),
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

    private static final RowsetStageLocation STAGE_LOCATION_STORAGE_ENDPOINT = new RowsetStageLocation(
            null,
            0,
            LocationType.S3.name(),
            false,
            STAGE_LOCATION_FORMAT.formatted(STAGE_LOCATION_BUCKET, STAGE_LOCATION_PATH),
            STAGE_LOCATION_BUCKET,
            STAGE_LOCATION_PATH,
            STAGE_LOCATION_REGION,
            null,
            STORAGE_ENDPOINT,
            null,
            CREDENTIALS
    );

    private static final EncryptionVersion ENCRYPTION_VERSION = EncryptionVersion.AES_CTR_128_V1;

    private static final String IV = "1234567890";

    private static final String DIVERSIFIER = "01";

    private static final String PIPE_KEY_ID = "1";

    private static final String ENTITY_TAG = "Entity-Tag";

    private static final String ENTITY_TAG_HEADER = "\"%s\"".formatted(ENTITY_TAG);

    private static final int FILE_FRAGMENTS = 5;

    private static final Pattern AUTHORIZATION_PATTERN = Pattern.compile("^AWS4-HMAC-SHA256 Credential=([^,]+), SignedHeaders=([^,]+), Signature=[a-f0-9]{64}$");

    private static final int CREDENTIAL_GROUP = 1;

    private static final int SIGNED_HEADERS_GROUP = 2;

    private static final String SIGNED_HEADERS_EXPECTED = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token";

    private static final String CURRENT_DATE = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC).format(Instant.now());

    private static final String CREDENTIAL_EXPECTED = "%s/%s/%s/s3/aws4_request".formatted(AWS_KEY_ID, CURRENT_DATE, STAGE_LOCATION_REGION);

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
    private ArgumentCaptor<String> authorizationCaptor;

    @Captor
    private ArgumentCaptor<URI> uriCaptor;

    private S3ObjectTransferClient objectTransferClient;

    @BeforeEach
    void setObjectTransferClient() {
        objectTransferClient = new S3ObjectTransferClient(webClientService, bufferEncryptor);
    }

    @Test
    void testPutFileFragment() {
        final ObjectTransferLocation objectTransferLocation = new ObjectTransferLocation(STAGE_LOCATION, pipeEncryptionInfo);

        putFileFragments(objectTransferLocation);
        verifyRequests();
        verifyRequestUri(STANDARD_ENDPOINT);
    }

    @Test
    void testPutFileFragmentStorageEndpointProvided() {
        final ObjectTransferLocation objectTransferLocation = new ObjectTransferLocation(STAGE_LOCATION_STORAGE_ENDPOINT, pipeEncryptionInfo);

        putFileFragments(objectTransferLocation);
        verifyRequests();
        verifyRequestUri(BUCKET_STORAGE_ENDPOINT);
    }

    private void putFileFragments(final ObjectTransferLocation objectTransferLocation) {
        final byte[] fileFragment = NDJSON_ROW.getBytes(StandardCharsets.UTF_8);
        final long uncompressedLengthBytes = fileFragment.length;
        final EncryptedBuffer encryptedBuffer = setEncryptedBuffer(fileFragment);
        setResponseEntity();

        for (int fragmentNumber = 0; fragmentNumber < FILE_FRAGMENTS; fragmentNumber++) {
            final FileFragmentInfo fileFragmentInfo = objectTransferClient.putFileFragment(fileFragment, ROW_COUNT, uncompressedLengthBytes, objectTransferLocation, OFFSET_TOKEN);
            assertFileFragmentInfoEquals(fileFragmentInfo, encryptedBuffer, fileFragment);
        }
    }

    private void verifyRequests() {
        verify(requestBodySpec, times(FILE_FRAGMENTS)).header(eq(HttpHeaderName.AUTHORIZATION.getHeaderName()), authorizationCaptor.capture());
        final List<String> authorizationHeaders = authorizationCaptor.getAllValues();
        final String authorization = authorizationHeaders.getFirst();
        assertAuthorizationHeaderFound(authorization);
    }

    private void verifyRequestUri(final String hostExpected) {
        verify(requestUriSpec, times(FILE_FRAGMENTS)).uri(uriCaptor.capture());
        final URI requestUri = uriCaptor.getValue();
        assertEquals(hostExpected, requestUri.getHost());
    }

    private void assertAuthorizationHeaderFound(final String authorization) {
        final Matcher matcher = AUTHORIZATION_PATTERN.matcher(authorization);
        assertTrue(matcher.matches());

        final String credentialGroup = matcher.group(CREDENTIAL_GROUP);
        assertEquals(CREDENTIAL_EXPECTED, credentialGroup);

        final String signedHeaders = matcher.group(SIGNED_HEADERS_GROUP);
        assertEquals(SIGNED_HEADERS_EXPECTED, signedHeaders);
    }

    private EncryptedBuffer setEncryptedBuffer(final byte[] fileFragment) {
        final byte[] buffer = Base64.getEncoder().encode(fileFragment);
        final EncryptedBuffer encryptedBuffer = new EncryptedBuffer(buffer, IV, DIVERSIFIER, PIPE_KEY_ID, ENCRYPTION_VERSION.getVersion());
        when(bufferEncryptor.encryptBuffer(eq(fileFragment), eq(pipeEncryptionInfo))).thenReturn(encryptedBuffer);
        return encryptedBuffer;
    }

    private void setResponseEntity() {
        when(webClientService.put()).thenReturn(requestUriSpec);

        when(requestUriSpec.uri(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.body(any(InputStream.class), any(OptionalLong.class))).thenReturn(requestBodySpec);
        when(requestBodySpec.header(any(), any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseEntity);

        when(responseEntity.statusCode()).thenReturn(HttpURLConnection.HTTP_OK);
        when(responseEntity.headers()).thenReturn(responseHeaders);
        when(responseHeaders.getFirstHeader(eq(HttpHeaderName.ETAG.getHeaderName()))).thenReturn(Optional.of(ENTITY_TAG_HEADER));
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
