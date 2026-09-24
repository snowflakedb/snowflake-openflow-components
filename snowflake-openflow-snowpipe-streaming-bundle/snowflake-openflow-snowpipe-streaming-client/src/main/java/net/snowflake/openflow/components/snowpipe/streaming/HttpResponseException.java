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
 * HTTP Response Exception related to failures reported in HTTP responses
 */
public class HttpResponseException extends RuntimeException {
    private final int statusCode;

    /**
     * HTTP Response Exception with standard HTTP request properties
     *
     * @param message Failure message
     * @param uri HTTP Request URI
     * @param requestMethod HTTP Request Method
     * @param statusCode HTTP Response Status Code
     */
    public HttpResponseException(
            final String message,
            final URI uri,
            final String requestMethod,
            final int statusCode
    ) {
        super(getMessage(message, uri, requestMethod, statusCode));
        this.statusCode = statusCode;
    }

    /**
     * Get HTTP Status Code
     *
     * @return HTTP Status Code
     */
    public int getStatusCode() {
        return statusCode;
    }

    private static String getMessage(final String message, final URI uri, final String requestMethod, final int statusCode) {
        return String.format("%s HTTP Method [%s] URI [%s] Response Status [%d]", message, requestMethod, uri, statusCode);
    }
}
