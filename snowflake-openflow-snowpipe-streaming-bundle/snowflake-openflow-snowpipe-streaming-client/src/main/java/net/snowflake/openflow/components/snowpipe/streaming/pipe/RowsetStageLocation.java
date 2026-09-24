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

import java.util.Map;

/**
 * Rowset Stage Location for Object Storage
 *
 * @param expiryTime Expiration Timestamp for Stage Location information
 * @param expiryTimeMs Expiration Time in epoch milliseconds
 * @param locationType Location Type such as S3
 * @param useS3RegionalUrl Flag for using S3 Regional URL
 * @param location Stage Location including Bucket and Directory
 * @param bucketName Object Storage Bucket Name parsed from Stage Location or empty
 * @param pathPrefix Object Path Prefix parsed from Stage Location or empty
 * @param region Object Storage region
 * @param storageAccount Object Storage Account
 * @param storageEndpoint Object Storage Endpoint
 * @param urlOverride Optional URL provided in Snowpark Container Services
 * @param creds Credentials for Object Storage access
 */
public record RowsetStageLocation(
        String expiryTime,
        long expiryTimeMs,
        String locationType,
        boolean useS3RegionalUrl,
        String location,
        String bucketName,
        String pathPrefix,
        String region,
        String storageAccount,
        String storageEndpoint,
        String urlOverride,
        Map<String, String> creds
) {
    @Override
    public String toString() {
        return "RowsetStageLocation[location=%s]".formatted(location);
    }
}
