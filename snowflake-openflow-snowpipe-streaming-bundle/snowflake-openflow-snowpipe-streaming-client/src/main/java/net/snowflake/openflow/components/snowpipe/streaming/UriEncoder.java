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
import java.net.URISyntaxException;

/**
 * Path segment encoding methods for RFC 3986 Section 2
 */
final class UriEncoder {

    private static final String PATH_SEPARATOR = "/";
    private static final String PATH_SEPARATOR_ENCODED = "%2F";
    private static final String SEMICOLON = ";";
    private static final String SEMICOLON_ENCODED = "%3B";

    private UriEncoder() {
    }

    /**
     * Get path segment with percent character encoding according to RFC 3986 Section 2
     *
     * @param segment Path segment to be encoded
     * @return Encoded path segment or unchanged when containing unreserved characters
     */
    static String getSegmentEncoded(final String segment) {
        // Prefix segment with Path Separator to handle scheme separator characters
        final String path = PATH_SEPARATOR + segment;
        try {
            final URI uri = new URI(null, null, path, null);
            final String rawPath = uri.getRawPath();
            final String encoded = rawPath.substring(1);
            return encoded.replace(PATH_SEPARATOR, PATH_SEPARATOR_ENCODED).replace(SEMICOLON, SEMICOLON_ENCODED);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Path segment [%s] encoding failed".formatted(segment), e);
        }
    }
}
