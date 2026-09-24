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

package net.snowflake.openflow.processors.snowpipe.streaming.property;

import org.apache.nifi.components.DescribedValue;

public enum TransferStrategy implements DescribedValue {
    /** Rows Buffer Size is 15 MB to avoid the maximum request limit of 16 MB */
    ROWS("Rows", "Transfer records as batches of rows over HTTP to Snowpipe Streaming", 15728640, 31457280),

    /** File Fragments Buffer Size is 64 MB aligning with an expected compressed size below 25 MB */
    FILE_FRAGMENTS("File Fragments", "Transfer records as file fragments over HTTP to cloud storage services", 67108864, 134217728),

    /** Managed Buffer Size is 64 MB aligning with an expected compressed size below 25 MB */
    MANAGED("Managed", "Transfer records as either batches of rows or file fragments based on uncompressed size", 67108864, 134217728);

    private final String displayName;

    private final String description;

    private final int bufferSize;

    private final int batchSize;

    TransferStrategy(final String displayName, final String description, final int bufferSize, final int batchSize) {
        this.displayName = displayName;
        this.description = description;
        this.bufferSize = bufferSize;
        this.batchSize = batchSize;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Get Buffer Size in bytes limiting the size of a single buffer before insert rows
     *
     * @return Buffer Size in bytes
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Get Batch Size in bytes limiting the size of collected FlowFiles for processing
     *
     * @return Batch Size in bytes
     */
    public int getBatchSize() {
        return batchSize;
    }
}
