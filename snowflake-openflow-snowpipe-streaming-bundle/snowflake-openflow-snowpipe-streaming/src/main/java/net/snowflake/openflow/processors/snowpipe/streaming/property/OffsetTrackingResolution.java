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

public enum OffsetTrackingResolution implements DescribedValue {
    RECORD("Record", "Track each Record in each FlowFile with monotonically increasing Offset Tokens"),

    FLOW_FILE("FlowFile", "Track each FlowFile with monotonically increasing Offset Tokens"),

    DISABLED("Disabled", "Opaque Offset Token handling without tracking across FlowFiles or Records");

    private final String displayName;

    private final String description;

    OffsetTrackingResolution(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
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
}
