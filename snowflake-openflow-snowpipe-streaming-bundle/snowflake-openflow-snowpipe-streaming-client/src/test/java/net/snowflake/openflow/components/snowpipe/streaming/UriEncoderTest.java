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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UriEncoderTest {

    private static final String UNRESERVED_CHARACTERS = "Unreserved_.~0-9";
    private static final String SUB_DELIMITERS = "!$&'()*+,=";

    private static final String SCHEME_SEPARATED = "\"path:segment/separated\"";
    private static final String SCHEME_SEPARATED_ENCODED = "%22path:segment%2Fseparated%22";

    private static final String RESERVED_WHITESPACE_CHARACTERS = "?#[ ]";
    private static final String RESERVED_WHITESPACE_ENCODED = "%3F%23%5B%20%5D";

    private static final String OTHER_CHARACTERS = "\"<>{}|`;";
    private static final String OTHER_ENCODED = "%22%3C%3E%7B%7D%7C%60%3B";

    @Test
    void testGetSegmentEncodedUnreservedUnchanged() {
        final String encoded = UriEncoder.getSegmentEncoded(UNRESERVED_CHARACTERS);
        assertEquals(UNRESERVED_CHARACTERS, encoded);
    }

    @Test
    void testGetSegmentEncodedSubDelimitersUnchanged() {
        final String encoded = UriEncoder.getSegmentEncoded(SUB_DELIMITERS);
        assertEquals(SUB_DELIMITERS, encoded);
    }

    @Test
    void testGetSegmentEncodedSchemeSeparatorUnchanged() {
        final String encoded = UriEncoder.getSegmentEncoded(SCHEME_SEPARATED);
        assertEquals(SCHEME_SEPARATED_ENCODED, encoded);
    }

    @Test
    void testGetSegmentEncodedReservedWhitespaceChanged() {
        final String encoded = UriEncoder.getSegmentEncoded(RESERVED_WHITESPACE_CHARACTERS);
        assertEquals(RESERVED_WHITESPACE_ENCODED, encoded);
    }

    @Test
    void testGetSegmentEncodedOtherChanged() {
        final String encoded = UriEncoder.getSegmentEncoded(OTHER_CHARACTERS);
        assertEquals(OTHER_ENCODED, encoded);
    }
}
