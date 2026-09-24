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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnowflakeManagedTokenAuthorizationProviderTest {

    private static final String SERVICE_ACCOUNT_TOKEN = "service-account-token";

    private static final String SESSION_TOKEN = "session-token";

    private static final String ACCOUNT_TOKEN = "ACCOUNT-TOKEN";
    private static final String ACCOUNT_TOKEN_AUTHORIZATION = "Bearer WIF.OIDC.%s".formatted(ACCOUNT_TOKEN);

    private static final String TOKEN = "TOKEN";
    private static final String TOKEN_AUTHORIZATION = "Bearer %s".formatted(TOKEN);

    @Test
    void testGetAuthorizationServiceAccountTokenPreferred(final @TempDir Path tempDir) throws IOException {
        final Path serviceAccountToken = tempDir.resolve(SERVICE_ACCOUNT_TOKEN);
        Files.writeString(serviceAccountToken, ACCOUNT_TOKEN);

        final Path sessionToken = tempDir.resolve(SESSION_TOKEN);
        Files.writeString(sessionToken, TOKEN);

        final SnowflakeManagedTokenAuthorizationProvider provider = new SnowflakeManagedTokenAuthorizationProvider(serviceAccountToken, sessionToken);

        final RequestAuthorization requestAuthorization = provider.getRequestAuthorization();

        assertNotNull(requestAuthorization);

        final String authorization = requestAuthorization.authorization();
        assertEquals(ACCOUNT_TOKEN_AUTHORIZATION, authorization);
    }

    @Test
    void testGetAuthorizationSessionTokenSelected(final @TempDir Path tempDir) throws IOException {
        final Path serviceAccountToken = tempDir.resolve(SERVICE_ACCOUNT_TOKEN);

        final Path sessionToken = tempDir.resolve(SESSION_TOKEN);
        Files.writeString(sessionToken, TOKEN);

        final SnowflakeManagedTokenAuthorizationProvider provider = new SnowflakeManagedTokenAuthorizationProvider(serviceAccountToken, sessionToken);

        final RequestAuthorization requestAuthorization = provider.getRequestAuthorization();

        assertNotNull(requestAuthorization);

        final String authorization = requestAuthorization.authorization();
        assertEquals(TOKEN_AUTHORIZATION, authorization);
    }

    @Test
    void testGetAuthorizationTokenNotFound(final @TempDir Path tempDir) {
        final Path serviceAccountToken = tempDir.resolve(SERVICE_ACCOUNT_TOKEN);
        final Path sessionToken = tempDir.resolve(SESSION_TOKEN);

        final SnowflakeManagedTokenAuthorizationProvider provider = new SnowflakeManagedTokenAuthorizationProvider(serviceAccountToken, sessionToken);

        assertThrows(IllegalStateException.class, provider::getRequestAuthorization);
    }
}
