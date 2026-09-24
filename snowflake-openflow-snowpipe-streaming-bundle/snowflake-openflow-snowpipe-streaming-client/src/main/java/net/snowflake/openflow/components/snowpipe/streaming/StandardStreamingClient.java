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
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Standard implementation of Snowpipe Streaming Client using Web Client Service for requests
 */
public class StandardStreamingClient implements StreamingClient {
    private static final String STREAMING_HOSTNAME_PATH = "/v2/streaming/hostname";

    private static final String UNDERSCORE_CHARACTER = "_";

    private static final String HYPHEN_CHARACTER = "-";

    private final WebClientService webClientService;

    private final RequestAuthorizationProvider requestAuthorizationProvider;

    private final URI accountUri;

    public StandardStreamingClient(
            final WebClientService webClientService,
            final RequestAuthorizationProvider requestAuthorizationProvider,
            final URI accountUri
    ) {
        this.webClientService = webClientService;
        this.requestAuthorizationProvider = requestAuthorizationProvider;
        this.accountUri = accountUri;
    }

    @Override
    public String getHostname() {
        final URI uri = accountUri.resolve(STREAMING_HOSTNAME_PATH);
        final RequestAuthorization requestAuthorization = requestAuthorizationProvider.getRequestAuthorization();

        try (HttpResponseEntity httpResponseEntity = webClientService.get()
                .uri(uri)
                .header(HttpHeaderName.ACCEPT.getHeaderName(), MediaType.APPLICATION_JSON.getMediaType())
                .header(HttpHeaderName.AUTHORIZATION.getHeaderName(), requestAuthorization.authorization())
                .header(HttpHeaderName.USER_AGENT.getHeaderName(), UserAgentProvider.getUserAgent())
                .retrieve()) {

            final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try (InputStream responseBody = httpResponseEntity.body()) {
                responseBody.transferTo(outputStream);
            }

            final String responseBody = outputStream.toString(StandardCharsets.UTF_8);
            final int statusCode = httpResponseEntity.statusCode();
            if (HttpURLConnection.HTTP_OK == statusCode) {
                // Normalize hostname for compliance with Domain Name Service and URI conventions
                return responseBody.replaceAll(UNDERSCORE_CHARACTER, HYPHEN_CHARACTER).toLowerCase(Locale.ROOT);
            } else {
                throw new HttpResponseException("Response [%s]".formatted(responseBody), uri, StandardHttpRequestMethod.GET.getMethod(), statusCode);
            }
        } catch (final IOException e) {
            throw new HttpClientException("Request communication failed", e, uri, StandardHttpRequestMethod.GET.getMethod());
        }
    }
}
