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
 * Pipe Information
 *
 * @param statusCode Status Code is 0 for success
 * @param message Status message is null for success
 * @param rowsetStageLocation Rowset Stage Location for Object Storage
 * @param pipeEncryptionInfo Pipe Encryption Information
 * @param durableStageLocation Durable Stage Location for Object Storage with an extended Time To Live or null when not provided
 */
public record PipeInfo(
        long statusCode,
        String message,
        RowsetStageLocation rowsetStageLocation,
        PipeEncryptionInfo pipeEncryptionInfo,
        RowsetStageLocation durableStageLocation
) {
}
