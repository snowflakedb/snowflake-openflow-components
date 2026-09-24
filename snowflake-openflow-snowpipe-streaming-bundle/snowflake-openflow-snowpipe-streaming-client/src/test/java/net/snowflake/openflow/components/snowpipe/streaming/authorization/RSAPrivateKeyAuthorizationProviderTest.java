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

package net.snowflake.openflow.components.snowpipe.streaming.authorization;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.interfaces.RSAPublicKey;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RSAPrivateKeyAuthorizationProviderTest {

    private static final String KEY_ALGORITHM = "RSA";

    private static final String SIGNING_ALGORITHM = "SHA256withRSA";

    private static final String ACCOUNT = "SNOWFLAKE-OPENFLOW";

    private static final String USER = "USER";

    private static final String ROLE = "OPENFLOW";

    private static final String DEFAULT_ROLE = null;

    private static final Pattern BEARER_PATTERN = Pattern.compile("^Bearer ([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_-]+)$");

    private static final int FIRST_GROUP = 1;

    private static final int SECOND_GROUP = 2;

    private static final int SIGNATURE_GROUP = 3;

    private static final String HEADER = "{\"alg\":\"RS256\"}";

    private static final String HEADER_PAYLOAD_FORMAT = "%s.%s";

    private static final Pattern SUBJECT_PATTERN = Pattern.compile("\"sub\":\"([^.]+)\\.(\\w+)\"");

    private static final Pattern ISSUED_AT_PATTERN = Pattern.compile("\"iat\":(\\d+)");

    private static final Pattern SCOPE_ROLE_PATTERN = Pattern.compile("\"scp\":\"session:role:(\\w+)\"");

    private static final Base64.Decoder urlDecoder = Base64.getUrlDecoder();

    private static RSAPublicKey publicKey;

    private static RSAPrivateCrtKey privateKey;

    private RSAPrivateKeyAuthorizationProvider provider;

    @BeforeAll
    static void setKeyPair() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        publicKey = (RSAPublicKey) keyPair.getPublic();
        privateKey = (RSAPrivateCrtKey) keyPair.getPrivate();
    }

    @BeforeEach
    void setProvider() {
        provider = new RSAPrivateKeyAuthorizationProvider(ACCOUNT, USER, DEFAULT_ROLE, privateKey);
    }

    @Test
    void testGetAuthorization() throws GeneralSecurityException {
        provider = new RSAPrivateKeyAuthorizationProvider(ACCOUNT, USER, null, privateKey);
        final RequestAuthorization requestAuthorization = provider.getRequestAuthorization();

        assertNotNull(requestAuthorization);

        final ZonedDateTime issued = requestAuthorization.issued();
        assertNotNull(issued);

        final String authorization = requestAuthorization.authorization();
        assertNotNull(authorization);

        final Matcher matcher = BEARER_PATTERN.matcher(authorization);
        assertTrue(matcher.matches());

        final String headerGroup = matcher.group(FIRST_GROUP);
        final String headerDecoded = new String(urlDecoder.decode(headerGroup), StandardCharsets.UTF_8);
        assertEquals(HEADER, headerDecoded);

        final String payloadGroup = matcher.group(SECOND_GROUP);
        assertPayloadClaimsFound(payloadGroup, issued);

        final String signatureGroup = matcher.group(SIGNATURE_GROUP);
        final String headerPayload = HEADER_PAYLOAD_FORMAT.formatted(headerGroup, payloadGroup);

        assertSignatureVerified(signatureGroup, headerPayload);
    }

    @Test
    void testGetAuthorizationRoleConfigured() throws GeneralSecurityException {
        provider = new RSAPrivateKeyAuthorizationProvider(ACCOUNT, USER, ROLE, privateKey);
        final RequestAuthorization requestAuthorization = provider.getRequestAuthorization();

        assertNotNull(requestAuthorization);

        final ZonedDateTime issued = requestAuthorization.issued();
        assertNotNull(issued);

        final String authorization = requestAuthorization.authorization();
        assertNotNull(authorization);

        final Matcher matcher = BEARER_PATTERN.matcher(authorization);
        assertTrue(matcher.matches());

        final String headerGroup = matcher.group(FIRST_GROUP);
        final String headerDecoded = new String(urlDecoder.decode(headerGroup), StandardCharsets.UTF_8);
        assertEquals(HEADER, headerDecoded);

        final String payloadGroup = matcher.group(SECOND_GROUP);
        assertPayloadClaimsFound(payloadGroup, issued);

        assertPayloadScopeClaimFound(payloadGroup);

        final String signatureGroup = matcher.group(SIGNATURE_GROUP);
        final String headerPayload = HEADER_PAYLOAD_FORMAT.formatted(headerGroup, payloadGroup);

        assertSignatureVerified(signatureGroup, headerPayload);
    }

    private void assertPayloadClaimsFound(final String payloadGroup, final ZonedDateTime issued) {
        final String payloadDecoded = new String(urlDecoder.decode(payloadGroup), StandardCharsets.UTF_8);

        final Matcher subjectMatcher = SUBJECT_PATTERN.matcher(payloadDecoded);
        assertTrue(subjectMatcher.find(), "Payload Subject claim not found");
        final String accountGroup = subjectMatcher.group(FIRST_GROUP);
        assertEquals(ACCOUNT, accountGroup);

        final String userGroup = subjectMatcher.group(SECOND_GROUP);
        assertEquals(USER, userGroup);

        final Matcher issuedAtMatcher = ISSUED_AT_PATTERN.matcher(payloadDecoded);
        assertTrue(issuedAtMatcher.find(), "Payload Issued At claim not found");
        final String issuedAtGroup = issuedAtMatcher.group(FIRST_GROUP);
        assertEquals(Long.toString(issued.toEpochSecond()), issuedAtGroup);
    }

    private void assertPayloadScopeClaimFound(final String payloadGroup) {
        final String payloadDecoded = new String(urlDecoder.decode(payloadGroup), StandardCharsets.UTF_8);

        final Matcher scopeRoleMatcher = SCOPE_ROLE_PATTERN.matcher(payloadDecoded);
        assertTrue(scopeRoleMatcher.find(), "Payload Scope claim not found");
        final String roleGroup = scopeRoleMatcher.group(FIRST_GROUP);
        assertEquals(ROLE, roleGroup);
    }

    private void assertSignatureVerified(final String signatureGroup, final String headerPayload) throws GeneralSecurityException {
        final byte[] headerPayloadEncoded = headerPayload.getBytes(StandardCharsets.UTF_8);
        final byte[] signatureDecoded = urlDecoder.decode(signatureGroup);

        final Signature signature = Signature.getInstance(SIGNING_ALGORITHM);
        signature.initVerify(publicKey);
        signature.update(headerPayloadEncoded);

        final boolean verified = signature.verify(signatureDecoded);
        assertTrue(verified, "Signature verification failed");
    }
}
