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

package net.snowflake.openflow.components.snowpipe.streaming.transfer;

import net.snowflake.openflow.components.snowpipe.streaming.pipe.FileFragmentInfo;

/**
 * Abstraction for transferring File Fragments to Object Storage services
 */
public interface ObjectTransferClient {
    /**
     * Put File Fragment based on Object Transfer Location with optional Offset Token Tracking
     *
     * @param fileFragment File Fragment to be transferred
     * @param fragmentRowCount Count of rows contained in the File Fragment
     * @param uncompressedLengthBytes File Fragment length in bytes before compression
     * @param objectTransferLocation Object Transfer Location
     * @param offsetToken Offset Token or null when tracking not required
     * @return File Fragment Information
     */
    FileFragmentInfo putFileFragment(byte[] fileFragment, long fragmentRowCount, long uncompressedLengthBytes, ObjectTransferLocation objectTransferLocation, String offsetToken);
}
