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

/**
 * Enumeration of Google Cloud Storage Credential Property names
 */
public enum GoogleCloudCredentialProperty {
    GCS_ACCESS_TOKEN("GCS_ACCESS_TOKEN");

    private final String property;

    GoogleCloudCredentialProperty(final String property) {
        this.property = property;
    }

    String getProperty() {
        return property;
    }
}
