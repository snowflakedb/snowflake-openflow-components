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

import java.net.URI;

/**
 * HTTP Client Exception related to web service operations
 */
public class HttpClientException extends RuntimeException {
    /**
     * HTTP Client Exception with standard HTTP request properties
     *
     * @param message Failure message
     * @param cause Failure cause
     * @param uri HTTP Request URI
     * @param requestMethod HTTP Request Method
     */
    public HttpClientException(
            final String message,
            final Throwable cause,
            final URI uri,
            final String requestMethod
    ) {
        super(getMessage(message, uri, requestMethod), cause);
    }

    private static String getMessage(final String message, final URI uri, final String requestMethod) {
        return String.format("%s HTTP Method [%s] URI [%s]", message, requestMethod, uri);
    }
}
