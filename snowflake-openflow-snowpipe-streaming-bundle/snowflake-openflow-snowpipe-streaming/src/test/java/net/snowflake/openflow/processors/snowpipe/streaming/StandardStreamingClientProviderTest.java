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

import net.snowflake.openflow.processors.snowpipe.streaming.connection.AuthenticationStrategy;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ConnectionStrategy;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StandardStreamingClientProviderTest {

    private static final String ACCOUNT = "orgname-accountname";

    private TestRunner runner;

    private StandardStreamingClientProvider provider;

    @BeforeEach
    void setUp() {
        provider = new StandardStreamingClientProvider();
        final PublishSnowpipeStreaming processor = new PublishSnowpipeStreaming();
        runner = TestRunners.newTestRunner(processor);
        runner.setProperty(StandardStreamingClientProvider.AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR);
        runner.setProperty(StandardStreamingClientProvider.ACCOUNT, ACCOUNT);
    }

    @Test
    void testGetAccountUriStandard() {
        assertAccountUri(ACCOUNT, null, "https://orgname-accountname.snowflakecomputing.com");
    }

    @Test
    void testGetAccountUriPrivateConnectivity() {
        assertAccountUri(ACCOUNT, ConnectionStrategy.PRIVATE_CONNECTIVITY, "https://orgname-accountname.privatelink.snowflakecomputing.com");
    }

    @Test
    void testGetAccountUriWithUnderscoresStandard() {
        assertAccountUri("org_name-account_name", null, "https://org-name-account-name.snowflakecomputing.com");
    }

    @Test
    void testGetAccountUriWithUnderscoresPrivateConnectivity() {
        assertAccountUri("org_name-account_name", ConnectionStrategy.PRIVATE_CONNECTIVITY, "https://org-name-account-name.privatelink.snowflakecomputing.com");
    }

    private void assertAccountUri(final String account, final ConnectionStrategy connectionStrategy, final String expectedUri) {
        runner.setProperty(StandardStreamingClientProvider.ACCOUNT, account);
        if (connectionStrategy != null) {
            runner.setProperty(StandardStreamingClientProvider.CONNECTION_STRATEGY, connectionStrategy);
        }

        final URI accountUri = provider.getAccountUri(runner.getProcessContext());

        assertEquals(expectedUri, accountUri.toString());
    }
}
