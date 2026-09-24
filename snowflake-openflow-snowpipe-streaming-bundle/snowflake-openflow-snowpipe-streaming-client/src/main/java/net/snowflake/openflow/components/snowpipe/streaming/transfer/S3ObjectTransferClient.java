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
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Amazon S3 implementation of Object Transfer Client using AWS Signature Version 4
 */
class S3ObjectTransferClient implements ObjectTransferClient {
    private static final String S3_SERVICE_TYPE = "s3";

    private static final String S3_URI_FORMAT = "https://%s.s3.%s.amazonaws.com/%s";

    private static final String STORAGE_ENDPOINT_URI_FORMAT = "https://%s.%s/%s";

    private static final String SIGNING_KEY_FORMAT = "AWS4%s";

    private static final String SIGNING_KEY_REQUEST_TYPE = "aws4_request";

    private static final String HMAC_ALGORITHM = "HmacSHA256";

    private static final String HASH_ALGORITHM = "SHA-256";

    private static final String CREDENTIAL_SCOPE_FORMAT = "%s/%s/s3/aws4_request";

    private static final String OBJECT_KEY_FORMAT = "%s%s.ndjson";

    private static final DateTimeFormatter AMZ_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private static final DateTimeFormatter CREDENTIAL_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private static final String CANONICAL_PATH_FORMAT = "/%s";

    private static final String CANONICAL_HEADERS_FORMAT = "content-type:%s\nhost:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\nx-amz-security-token:%s\n";

    private static final String SORTED_HEADER_NAMES = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token";

    private static final String CANONICAL_REQUEST_FORMAT = "PUT\n%s\n\n%s\n%s\n%s";

    private static final String SIGNATURE_PAYLOAD_FORMAT = "AWS4-HMAC-SHA256\n%s\n%s\n%s";

    private static final String AUTHORIZATION_FORMAT = "AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s";

    private static final long START_BYTE_OFFSET = 0;

    private static final String BUCKET_FILE_PATH_FORMAT = "%s/%s";

    private static final Pattern ENTITY_TAG_PATTERN = Pattern.compile("^\"(.+?)\"$");

    private static final int FIRST_GROUP = 1;

    private final WebClientService webClientService;

    private final BufferEncryptor bufferEncryptor;

    public S3ObjectTransferClient(final WebClientService webClientService, final BufferEncryptor bufferEncryptor) {
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
        final String sessionToken = credentials.get(S3CredentialProperty.AWS_TOKEN.getProperty());
        if (sessionToken == null) {
            throw new IllegalStateException("AWS Session Token not found in Rowset Stage Location");
        }

        final String secretKey = credentials.get(S3CredentialProperty.AWS_SECRET_KEY.getProperty());
        if (secretKey == null) {
            throw new IllegalStateException("AWS Secret Key not found in Rowset Stage Location");
        }

        final String accessKeyId = credentials.get(S3CredentialProperty.AWS_KEY_ID.getProperty());
        if (accessKeyId == null) {
            throw new IllegalStateException("AWS Access Key not found in Rowset Stage Location");
        }

        final String region = stageLocation.region();

        final String location = stageLocation.location();
        final String[] bucketPrefix = location.split("/", 2);
        final String bucket = bucketPrefix[0];
        final String prefix = bucketPrefix[1];
        // Set random UUID for filename to avoid potential conflicts in the bucket location
        final String objectKey = OBJECT_KEY_FORMAT.formatted(prefix, UUID.randomUUID());

        // Storage Endpoint overrides Bucket and Region URL when provided
        final String storageEndpoint = stageLocation.storageEndpoint();
        final String serviceUri;
        if (storageEndpoint == null) {
            serviceUri = S3_URI_FORMAT.formatted(bucket, region, objectKey);
        } else {
            serviceUri = STORAGE_ENDPOINT_URI_FORMAT.formatted(bucket, storageEndpoint, objectKey);
        }
        final URI uri = URI.create(serviceUri);

        final String amzDate = AMZ_DATE_FORMATTER.format(Instant.now());

        final EncryptedBuffer encryptedBuffer = getEncryptedBuffer(fileFragment, objectTransferLocation);
        final byte[] payload = encryptedBuffer.buffer();

        final String payloadHash = getEncodedHash(payload);
        final String canonicalHeaders = CANONICAL_HEADERS_FORMAT.formatted(MediaType.APPLICATION_NDJSON.getMediaType(), uri.getHost(), payloadHash, amzDate, sessionToken);

        final String canonicalPath = CANONICAL_PATH_FORMAT.formatted(objectKey);
        final String canonicalRequest = CANONICAL_REQUEST_FORMAT.formatted(canonicalPath, canonicalHeaders, SORTED_HEADER_NAMES, payloadHash);
        final String canonicalRequestHash = getEncodedHash(canonicalRequest.getBytes(StandardCharsets.UTF_8));

        final String credentialDate = CREDENTIAL_DATE_FORMATTER.format(Instant.now());
        final String credentialScope = CREDENTIAL_SCOPE_FORMAT.formatted(credentialDate, region);

        final String signaturePayload = SIGNATURE_PAYLOAD_FORMAT.formatted(amzDate, credentialScope, canonicalRequestHash);
        final byte[] signatureKey = getSignatureKey(secretKey, credentialDate, region);
        final byte[] signature = getKeyedHash(signatureKey, signaturePayload);
        final String signatureEncoded = HexFormat.of().formatHex(signature);

        final String authorization = AUTHORIZATION_FORMAT.formatted(accessKeyId, credentialScope, SORTED_HEADER_NAMES, signatureEncoded);

        final HttpRequestBodySpec requestBodySpec = webClientService.put()
                .uri(uri)
                .body(new ByteArrayInputStream(payload), OptionalLong.of(payload.length))
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_NDJSON.getMediaType())
                .header(AmazonHttpHeader.CONTENT_SHA256.getHeader(), payloadHash)
                .header(AmazonHttpHeader.DATE.getHeader(), amzDate)
                .header(AmazonHttpHeader.SECURITY_TOKEN.getHeader(), sessionToken)
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
                throw new HttpResponseException("S3 PUT Failed [%s]".formatted(responseBody), uri, StandardHttpRequestMethod.PUT.getMethod(), statusCode);
            }
        } catch (final IOException e) {
            throw new HttpClientException("S3 Request communication failed", e, uri, StandardHttpRequestMethod.PUT.getMethod());
        }

        final long fileLengthBytes = payload.length;

        final String filePath = BUCKET_FILE_PATH_FORMAT.formatted(bucket, objectKey);
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
        final String entityTag = entityTagHeader.map(ENTITY_TAG_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(matcher -> matcher.group(FIRST_GROUP))
                .orElse(null);

        if (entityTag == null) {
            throw new IllegalStateException("S3 ETag header not found");
        }
        return entityTag;
    }

    private String getEncodedHash(final byte[] bytes) {
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("Hash Algorithm [%s] not supported".formatted(HASH_ALGORITHM), e);
        }

        final byte[] hash = messageDigest.digest(bytes);
        return HexFormat.of().formatHex(hash);
    }

    private byte[] getKeyedHash(final byte[] key, final String input) {
        final Mac mac = getMac(key);
        final byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
        return mac.doFinal(inputBytes);
    }

    private Mac getMac(final byte[] key) {
        try {
            final Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            final SecretKey secretKey = new SecretKeySpec(key, mac.getAlgorithm());
            mac.init(secretKey);
            return mac;
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("Message Authentication Code [%s] initialization failed".formatted(HMAC_ALGORITHM), e);
        }
    }

    private byte[] getSignatureKey(final String secretKey, final String dateStamp, final String region) {
        final byte[] signingKey = SIGNING_KEY_FORMAT.formatted(secretKey).getBytes(StandardCharsets.UTF_8);
        final byte[] dateKey = getKeyedHash(signingKey, dateStamp);
        final byte[] regionKey = getKeyedHash(dateKey, region);
        final byte[] serviceKey = getKeyedHash(regionKey, S3_SERVICE_TYPE);
        return getKeyedHash(serviceKey, SIGNING_KEY_REQUEST_TYPE);
    }
}
