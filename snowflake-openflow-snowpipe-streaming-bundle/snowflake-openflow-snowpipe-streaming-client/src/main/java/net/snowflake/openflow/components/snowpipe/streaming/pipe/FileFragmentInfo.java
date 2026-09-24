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

package net.snowflake.openflow.components.snowpipe.streaming.pipe;

/**
 * File Fragment Information for inserting into Snowpipe Streaming
 *
 * @param filePath File Path for processing
 * @param contentType Content Type of file to be processed
 * @param fileEtag Entity Tag of file to be processed
 * @param fileLengthBytes File length in bytes to be processed
 * @param fragmentLengthBytes Fragment length in bytes
 * @param fragmentLengthUncompressedBytes Fragment length in bytes prior to compression
 * @param fragmentEndOffsetToken End Offset Token associated with File Fragment to be processed
 * @param fragmentRowCount Count of rows contained in File Fragment to be processed
 * @param fragmentStartByteOffset Start byte offset for processing
 * @param diversifier String used to stretch the Pipe Key for encryption
 * @param masterKeyId Pipe Key Identifier for encryption
 * @param ivBase64 Initialization Vector encoded in Base64 for encryption
 * @param encryptionVersion Encryption Version
 */
public record FileFragmentInfo(
        String filePath,
        String contentType,
        String fileEtag,
        long fileLengthBytes,
        long fragmentLengthBytes,
        long fragmentLengthUncompressedBytes,
        String fragmentEndOffsetToken,
        long fragmentRowCount,
        long fragmentStartByteOffset,
        String diversifier,
        String masterKeyId,
        String ivBase64,
        String encryptionVersion
) {
}
