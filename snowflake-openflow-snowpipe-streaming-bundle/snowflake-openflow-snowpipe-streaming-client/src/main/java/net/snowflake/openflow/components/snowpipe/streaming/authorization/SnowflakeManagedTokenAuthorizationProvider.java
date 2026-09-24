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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;

/**
 * Snowflake Managed Token Authorization Provider for Snowpark Container Services or Workload Identity Federation
 */
public class SnowflakeManagedTokenAuthorizationProvider implements RequestAuthorizationProvider {
    private static final Path SNOWFLAKE_SERVICE_ACCOUNT_TOKEN = Paths.get("/snowflake/serviceaccount/token");

    private static final Path SNOWFLAKE_SESSION_TOKEN = Paths.get("/snowflake/session/token");

    private static final String AUTHORIZATION_FORMAT = "Bearer %s";

    private static final String WORKLOAD_IDENTITY_TOKEN_TEMPLATE = "WIF.OIDC.%s";

    private final Path snowflakeServiceAccountToken;

    private final Path snowflakeSessionToken;

    public SnowflakeManagedTokenAuthorizationProvider() {
        this(SNOWFLAKE_SERVICE_ACCOUNT_TOKEN, SNOWFLAKE_SESSION_TOKEN);
    }

    SnowflakeManagedTokenAuthorizationProvider(final Path snowflakeServiceAccountToken, final Path snowflakeSessionToken) {
        this.snowflakeServiceAccountToken = snowflakeServiceAccountToken;
        this.snowflakeSessionToken = snowflakeSessionToken;
    }

    @Override
    public RequestAuthorization getRequestAuthorization() {
        final String snowflakeToken = readSnowflakeToken();
        final String authorization = AUTHORIZATION_FORMAT.formatted(snowflakeToken);
        final ZonedDateTime issued = ZonedDateTime.now();
        return new RequestAuthorization(authorization, issued);
    }

    private String readSnowflakeToken() {
        final Path snowflakeTokenLocation = getSnowflakeTokenLocation();

        try {
            final String token = Files.readString(snowflakeTokenLocation).strip();

            final String snowflakeToken;
            if (snowflakeServiceAccountToken.equals(snowflakeTokenLocation)) {
                snowflakeToken = WORKLOAD_IDENTITY_TOKEN_TEMPLATE.formatted(token);
            } else {
                snowflakeToken = token;
            }

            return snowflakeToken;
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to read Snowflake Token", e);
        }
    }

    private Path getSnowflakeTokenLocation() {
        final Path snowflakeTokenLocation;
        if (Files.isReadable(snowflakeServiceAccountToken)) {
            snowflakeTokenLocation = snowflakeServiceAccountToken;
        } else if (Files.isReadable(snowflakeSessionToken)) {
            snowflakeTokenLocation = snowflakeSessionToken;
        } else {
            throw new IllegalStateException("Service Account Token [%s] and Session Token [%s] not found".formatted(snowflakeServiceAccountToken, snowflakeSessionToken));
        }
        return snowflakeTokenLocation;
    }
}
