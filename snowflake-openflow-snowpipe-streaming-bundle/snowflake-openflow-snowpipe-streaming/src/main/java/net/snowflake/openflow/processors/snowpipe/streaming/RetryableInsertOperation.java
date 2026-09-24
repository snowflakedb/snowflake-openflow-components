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

package net.snowflake.openflow.processors.snowpipe.streaming;

import net.snowflake.openflow.components.snowpipe.streaming.HttpClientException;
import net.snowflake.openflow.components.snowpipe.streaming.HttpResponseException;
import net.snowflake.openflow.components.snowpipe.streaming.pipe.InsertStatus;
import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingChannel;
import org.apache.nifi.logging.ComponentLog;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Retryable implementation of Insert Operation based identified HTTP status codes
 */
class RetryableInsertOperation implements InsertOperation {
    private static final int SLEEP_MULTIPLIER = 2;

    private static final Duration INITIAL_SLEEP_DURATION = Duration.ofSeconds(1);

    private static final Set<Integer> RETRY_CLIENT_STATUS_CODES = Set.of(
            HttpStatus.REQUEST_TIMEOUT.status,
            HttpStatus.TOO_MANY_REQUESTS.status,
            HttpStatus.CLIENT_CLOSED_REQUEST.status
    );

    private final ComponentLog logger;

    private final StreamingChannel streamingChannel;

    private final InsertOperation insertOperation;

    private final Duration insertTimeout;

    RetryableInsertOperation(
            final Duration insertTimeout,
            final ComponentLog logger,
            final StreamingChannel streamingChannel,
            final InsertOperation insertOperation
    ) {
        this.insertTimeout = insertTimeout;
        this.logger = logger;
        this.streamingChannel = streamingChannel;
        this.insertOperation = insertOperation;
    }

    /**
     * Insert and return status
     *
     * @return Insert Status
     */
    @Override
    public InsertStatus insert() {
        final long insertTimeoutDuration = insertTimeout.toMillis();
        long sleepDuration = INITIAL_SLEEP_DURATION.toMillis();

        InsertStatus insertStatus = null;
        while (sleepDuration < insertTimeoutDuration) {
            try {
                insertStatus = insertOperation.insert();
                break;
            } catch (final Exception e) {
                if (e instanceof HttpResponseException responseException) {
                    final int statusCode = responseException.getStatusCode();
                    if (isRetrySupported(statusCode)) {
                        logger.warn("{} Insert failed with HTTP {}: waiting {} ms", streamingChannel, statusCode, sleepDuration, responseException);
                    } else {
                        throw e;
                    }
                } else if (e instanceof HttpClientException clientException) {
                    logger.warn("{} Insert failed: waiting {} ms", streamingChannel, sleepDuration, clientException);
                } else {
                    // Other Exceptions not retried
                    throw e;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(sleepDuration);
                } catch (final InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw e;
                }

                // Set Sleep Duration for subsequent retries
                sleepDuration *= SLEEP_MULTIPLIER;
            }
        }

        if (insertStatus == null) {
            throw new StreamingException("%s Insert failed after exceeding timeout of %d ms".formatted(streamingChannel, insertTimeoutDuration));
        }

        return insertStatus;
    }

    private boolean isRetrySupported(final int statusCode) {
        final boolean retrySupported;

        if (statusCode >= HttpStatus.INTERNAL_SERVER_ERROR.status) {
            // Retry HTTP server errors
            retrySupported = true;
        } else {
            // Retry selected HTTP client errors
            retrySupported = RETRY_CLIENT_STATUS_CODES.contains(statusCode);
        }

        return retrySupported;
    }

    private enum HttpStatus {
        REQUEST_TIMEOUT(408),

        TOO_MANY_REQUESTS(429),

        CLIENT_CLOSED_REQUEST(499),

        INTERNAL_SERVER_ERROR(500);

        final int status;

        HttpStatus(final int status) {
            this.status = status;
        }
    }
}
