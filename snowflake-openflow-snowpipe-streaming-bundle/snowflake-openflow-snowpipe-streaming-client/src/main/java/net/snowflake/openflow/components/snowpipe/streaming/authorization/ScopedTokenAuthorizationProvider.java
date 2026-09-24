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

import net.snowflake.openflow.components.snowpipe.streaming.HttpClientException;
import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.MediaType;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Authorization Provider that exchanges an existing Request Authorization Token for a scoped Access Token
 */
public final class ScopedTokenAuthorizationProvider implements RequestAuthorizationProvider {

    private static final String GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer";

    private static final String REQUEST_BODY_FORMAT = "grant_type=%s&scope=%s";

    private static final String OAUTH_TOKEN_PATH = "/oauth/token";

    private static final String BEARER_TOKEN_AUTHORIZATION_FORMAT = "Bearer %s";

    private static final Duration EXPIRATION = Duration.ofMinutes(59);

    private static final Duration REFRESH_EXPIRATION = EXPIRATION.minusMinutes(5);

    private final AtomicReference<RequestAuthorization> currentRequestAuthorization = new AtomicReference<>();

    private final RequestAuthorizationProvider requestAuthorizationProvider;

    private final String scope;

    private final WebClientService webClientService;

    private final URI accountUri;

    public ScopedTokenAuthorizationProvider(
            final RequestAuthorizationProvider requestAuthorizationProvider,
            final String scope,
            final WebClientService webClientService,
            final URI accountUri
    ) {
        this.requestAuthorizationProvider = Objects.requireNonNull(requestAuthorizationProvider, "Request Authorization Provider required");
        this.scope = Objects.requireNonNull(scope, "Scope required");
        this.webClientService = Objects.requireNonNull(webClientService, "Web Client Service required");
        this.accountUri = Objects.requireNonNull(accountUri, "Account URI required");
    }

    /**
     * Get Request Authorization based on exchanging JWT Bearer Token for a scoped Access Token with matching expiration
     *
     * @return Request Authorization containing scoped Access Token
     */
    @Override
    public RequestAuthorization getRequestAuthorization() {
        final RequestAuthorization requestAuthorization;

        final RequestAuthorization currentAuthorization = currentRequestAuthorization.get();
        if (isRefreshRequired(currentAuthorization)) {
            final RequestAuthorization scopedRequestAuthorization = getScopedRequestAuthorization();
            currentRequestAuthorization.set(scopedRequestAuthorization);
            requestAuthorization = scopedRequestAuthorization;
        } else {
            requestAuthorization = currentAuthorization;
        }

        return requestAuthorization;
    }

    private boolean isRefreshRequired(final RequestAuthorization requestAuthorization) {
        final boolean refreshRequired;

        if (requestAuthorization == null) {
            refreshRequired = true;
        } else {
            final ZonedDateTime issued = requestAuthorization.issued();
            final ZonedDateTime refreshExpiration = issued.plus(REFRESH_EXPIRATION);
            final ZonedDateTime now = ZonedDateTime.now();
            refreshRequired = now.isAfter(refreshExpiration);
        }

        return refreshRequired;
    }

    private RequestAuthorization getScopedRequestAuthorization() {
        final RequestAuthorization requestAuthorization = requestAuthorizationProvider.getRequestAuthorization();

        final String requestBody = REQUEST_BODY_FORMAT.formatted(GRANT_TYPE, scope);
        final URI uri = accountUri.resolve(OAUTH_TOKEN_PATH);

        final HttpRequestBodySpec requestBodySpec = webClientService.post()
                .uri(uri)
                .body(requestBody)
                .header(HttpHeaderName.AUTHORIZATION.getHeaderName(), requestAuthorization.authorization())
                .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), MediaType.APPLICATION_WWW_FORM_URLENCODED.getMediaType());

        try (HttpResponseEntity httpResponseEntity = requestBodySpec.retrieve()) {
            final int statusCode = httpResponseEntity.statusCode();

            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (InputStream responseBody = httpResponseEntity.body()) {
                responseBody.transferTo(outputStream);
            }
            final String responseBody = outputStream.toString(StandardCharsets.UTF_8);

            if (HttpURLConnection.HTTP_OK == statusCode) {
                // HTTP Response Body contains JSON Web Token
                final String authorization = BEARER_TOKEN_AUTHORIZATION_FORMAT.formatted(responseBody);
                // Scoped Authorization issued based on original Request Authorization to prompt expiration
                return new RequestAuthorization(authorization, requestAuthorization.issued());
            } else {
                throw new HttpResponseException("Response [%s]".formatted(responseBody), uri, StandardHttpRequestMethod.POST.getMethod(), statusCode);
            }
        } catch (final IOException e) {
            throw new HttpClientException("Request communication failed", e, uri, StandardHttpRequestMethod.POST.getMethod());
        }
    }
}
