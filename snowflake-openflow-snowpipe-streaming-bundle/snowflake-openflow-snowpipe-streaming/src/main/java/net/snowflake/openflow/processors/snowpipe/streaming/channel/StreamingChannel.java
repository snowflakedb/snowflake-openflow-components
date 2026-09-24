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

package net.snowflake.openflow.processors.snowpipe.streaming.channel;

import java.math.BigDecimal;

/**
 * Streaming Channel with destination and token tracking properties
 */
public class StreamingChannel {
    private final String database;

    private final String schema;

    private final String pipe;

    private final String channel;

    private volatile String continuationToken;

    private volatile String offsetToken;

    private volatile String committedOffsetToken;

    private volatile long rowsErrorCount;

    public StreamingChannel(final String database, final String schema, final String pipe, final String channel) {
        this.database = database;
        this.schema = schema;
        this.pipe = pipe;
        this.channel = channel;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getPipe() {
        return pipe;
    }

    public String getChannel() {
        return channel;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    public void setContinuationToken(final String continuationToken) {
        this.continuationToken = continuationToken;
    }

    /**
     * Get Offset Token contains local status for comparison against Channel Status committed Offset Token
     *
     * @return Local Offset Token
     */
    public String getOffsetToken() {
        return offsetToken;
    }

    public void setOffsetToken(final String offsetToken) {
        this.offsetToken = offsetToken;
    }

    public String getCommittedOffsetToken() {
        return committedOffsetToken;
    }

    /**
     * Set Committed Offset Token from latest version of Channel Status
     *
     * @param committedOffsetToken Committed Offset Token
     */
    public void setCommittedOffsetToken(final String committedOffsetToken) {
        this.committedOffsetToken = committedOffsetToken;
    }

    /**
     * Get Committed Offset Token as BigDecimal number
     *
     * @return Committed Offset Token Number defaults to 0 for null Offset Tokens
     */
    public BigDecimal getCommittedOffsetTokenNumber() {
        final BigDecimal committedOffsetTokenNumber;

        if (committedOffsetToken == null) {
            committedOffsetTokenNumber = BigDecimal.ZERO;
        } else {
            try {
                committedOffsetTokenNumber = new BigDecimal(committedOffsetToken);
            } catch (final NumberFormatException e) {
                throw new IllegalStateException("Committed Offset Token [%s] not valid for %s".formatted(committedOffsetToken, toString()));
            }
        }

        return committedOffsetTokenNumber;
    }

    /**
     * Get Rows Error Count since Channel creation
     *
     * @return Rows Error Count
     */
    public long getRowsErrorCount() {
        return rowsErrorCount;
    }

    /**
     * Set Rows Error Count since Channel creation based on latest Channel Status
     *
     * @param rowsErrorCount Rows Error Count
     */
    public void setRowsErrorCount(final long rowsErrorCount) {
        this.rowsErrorCount = rowsErrorCount;
    }

    @Override
    public String toString() {
        return "StreamingChannel [%s.%s.%s.%s]".formatted(database, schema, pipe, channel);
    }
}
