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
 * Insert Status
 *
 * @param statusCode Status Code is 0 on success
 * @param message Status message or null on success
 * @param nextContinuationToken Next Continuation Token for Channel required for subsequent insert operations
 */
public record InsertStatus(
        long statusCode,
        String message,
        String nextContinuationToken
) {
}
