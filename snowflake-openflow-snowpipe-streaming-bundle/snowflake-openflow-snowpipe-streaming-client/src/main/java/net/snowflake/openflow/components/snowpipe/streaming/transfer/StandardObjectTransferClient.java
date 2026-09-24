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
import net.snowflake.openflow.components.snowpipe.streaming.pipe.LocationType;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.RowsetStageLocation;
import net.snowflake.openflow.components.snowpipe.streaming.security.BufferEncryptor;
import org.apache.nifi.web.client.api.WebClientService;

import java.util.Objects;

/**
 * Standard Object Transfer Client supporting delegating to particular services based on Location Type
 */
public final class StandardObjectTransferClient implements ObjectTransferClient {
    private final S3ObjectTransferClient s3ObjectTransferClient;

    private final AzureObjectTransferClient azureObjectTransferClient;

    private final GoogleCloudObjectTransferClient googleCloudObjectTransferClient;

    public StandardObjectTransferClient(final WebClientService webClientService, final BufferEncryptor bufferEncryptor) {
        Objects.requireNonNull(webClientService, "Web Client Service required");
        Objects.requireNonNull(bufferEncryptor, "Buffer Encryptor required");
        this.s3ObjectTransferClient = new S3ObjectTransferClient(webClientService, bufferEncryptor);
        this.azureObjectTransferClient = new AzureObjectTransferClient(webClientService, bufferEncryptor);
        this.googleCloudObjectTransferClient = new GoogleCloudObjectTransferClient(webClientService, bufferEncryptor);
    }

    /**
     * Put File Fragment and send to service based on supported Location Type from the Object Transfer Location
     *
     * @param fileFragment File Fragment to be transferred
     * @param fragmentRowCount Count of rows contained in the File Fragment
     * @param uncompressedLengthBytes File Fragment length in bytes before compression
     * @param objectTransferLocation Object Transfer Location
     * @param offsetToken Offset Token or null when tracking not required
     * @return File Fragment Info
     * @throws IllegalStateException Thrown on unsupported Location Type
     */
    @Override
    public FileFragmentInfo putFileFragment(
            final byte[] fileFragment,
            final long fragmentRowCount,
            final long uncompressedLengthBytes,
            final ObjectTransferLocation objectTransferLocation,
            final String offsetToken
    ) {
        Objects.requireNonNull(objectTransferLocation, "Object Transfer Location required");
        final RowsetStageLocation stageLocation = objectTransferLocation.stageLocation();
        Objects.requireNonNull(stageLocation, "Stage Location required");

        final FileFragmentInfo fileFragmentInfo;
        final String locationType = stageLocation.locationType();
        if (LocationType.AZURE.name().equals(locationType)) {
            fileFragmentInfo = azureObjectTransferClient.putFileFragment(fileFragment, fragmentRowCount, uncompressedLengthBytes, objectTransferLocation, offsetToken);
        } else if (LocationType.GCS.name().equals(locationType)) {
            fileFragmentInfo = googleCloudObjectTransferClient.putFileFragment(fileFragment, fragmentRowCount, uncompressedLengthBytes, objectTransferLocation, offsetToken);
        } else if (LocationType.S3.name().equals(locationType)) {
            fileFragmentInfo = s3ObjectTransferClient.putFileFragment(fileFragment, fragmentRowCount, uncompressedLengthBytes, objectTransferLocation, offsetToken);
        } else {
            throw new IllegalStateException("Stage Location Type [%s] not supported".formatted(locationType));
        }

        return fileFragmentInfo;
    }
}
