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

import net.snowflake.openflow.components.snowpipe.streaming.StandardStreamingChannelClient;
import net.snowflake.openflow.components.snowpipe.streaming.StandardStreamingClient;
import net.snowflake.openflow.components.snowpipe.streaming.StreamingChannelClient;
import net.snowflake.openflow.components.snowpipe.streaming.StreamingClient;
import net.snowflake.openflow.components.snowpipe.streaming.authorization.RSAPrivateKeyAuthorizationProvider;
import net.snowflake.openflow.components.snowpipe.streaming.authorization.RequestAuthorizationProvider;
import net.snowflake.openflow.components.snowpipe.streaming.authorization.ScopedTokenAuthorizationProvider;
import net.snowflake.openflow.components.snowpipe.streaming.authorization.SnowflakeManagedTokenAuthorizationProvider;
import net.snowflake.openflow.processors.snowpipe.streaming.connection.AuthenticationStrategy;
import net.snowflake.openflow.processors.snowpipe.streaming.connection.SnowflakeEnvironmentVariable;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ConnectionStrategy;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.net.URI;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateCrtKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Standard implementation of Streaming Client Provider including shared Property Descriptors for configuration
 */
class StandardStreamingClientProvider implements StreamingClientProvider {
    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .description("Strategy for authenticating Snowflake connections")
            .required(true)
            .defaultValue(AuthenticationStrategy.KEY_PAIR)
            .allowableValues(AuthenticationStrategy.class)
            .build();

    static final PropertyDescriptor ACCOUNT = new PropertyDescriptor.Builder()
            .name("Account")
            .description("Snowflake Account Identifier with Organization Name and Account Name formatted as [organization-name]-[account-name]")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR)
            .build();

    static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
            .name("User")
            .description("Snowflake User for authenticating connections")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR)
            .build();

    static final PropertyDescriptor ROLE = new PropertyDescriptor.Builder()
            .name("Role")
            .description("Snowflake Role for authorizing operations on connections uses the default role for the User when not specified")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR)
            .build();

    static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("Private Key Service")
            .description("RSA Private Key Service for authenticating connections")
            .required(true)
            .identifiesControllerService(PrivateKeyService.class)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR)
            .build();

    static final PropertyDescriptor CONNECTION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Connection Strategy")
            .description("Strategy for connecting to Snowflake Snowpipe Streaming services")
            .required(true)
            .defaultValue(ConnectionStrategy.STANDARD)
            .allowableValues(ConnectionStrategy.class)
            .dependsOn(AUTHENTICATION_STRATEGY, AuthenticationStrategy.KEY_PAIR)
            .build();

    static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("Web Client Service Provider")
            .description("Web Client Service Provider supporting HTTP request and response handling")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    private static final String ACCOUNT_HOST_FORMAT = "https://%s.snowflakecomputing.com";
    private static final String ACCOUNT_PRIVATE_LINK_HOST_FORMAT = "https://%s.privatelink.snowflakecomputing.com";
    private static final String SNOWFLAKE_HOST_URI_FORMAT = "https://%s";
    private static final String STREAMING_URI_FORMAT = "https://%s";

    private static final String UNDERSCORE_CHARACTER = "_";
    private static final String HYPHEN_CHARACTER = "-";
    private static final Pattern HOST_PATTERN = Pattern.compile("^([^.]+?)\\..*");
    private static final int FIRST_GROUP = 1;

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog componentLog, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final RequestAuthorizationProvider requestAuthorizationProvider = getRequestAuthorizationProvider(context);

            final URI accountUri = getAccountUri(context);
            final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
            final WebClientService webClientService = webClientServiceProvider.getWebClientService();
            try {
                final StreamingClient streamingClient = new StandardStreamingClient(webClientService, requestAuthorizationProvider, accountUri);
                final URI accountStreamingUri = getStreamingUri(streamingClient);

                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName(VerificationStepName.STREAMING_CONNECTION.label)
                        .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                        .explanation("Streaming URI configured [%s]".formatted(accountStreamingUri))
                        .build()
                );
            } catch (final Exception e) {
                componentLog.warn("Failed to get Streaming Hostname from Account URI [%s]".formatted(accountUri), e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName(VerificationStepName.STREAMING_CONNECTION.label)
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation("Streaming Hostname configuration failed: %s".formatted(e.getMessage()))
                        .build()
                );
            }
        } catch (final Exception e) {
            componentLog.warn("Failed to get Request Authorization Provider", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName(VerificationStepName.ACCOUNT_CONFIGURATION.label)
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .explanation("Request Authorization Provider configuration failed: %s".formatted(e.getMessage()))
                    .build()
            );
        }

        return results;
    }

    @Override
    public URI getStreamingUri(final ProcessContext context) {
        final URI accountUri = getAccountUri(context);

        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        final WebClientService webClientService = webClientServiceProvider.getWebClientService();
        final RequestAuthorizationProvider requestAuthorizationProvider = getRequestAuthorizationProvider(context);
        final StreamingClient streamingClient = new StandardStreamingClient(webClientService, requestAuthorizationProvider, accountUri);
        try {
            return getStreamingUri(streamingClient);
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to get Streaming Hostname from Account URI [%s]".formatted(accountUri), e);
        }
    }

    @Override
    public StreamingChannelClient getStreamingChannelClient(final ProcessContext context, final URI streamingUri) {
        final URI accountUri = getAccountUri(context);

        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        final WebClientService webClientService = webClientServiceProvider.getWebClientService();
        final RequestAuthorizationProvider requestAuthorizationProvider = getRequestAuthorizationProvider(context);

        final RequestAuthorizationProvider channelRequestAuthorizationProvider;
        if (requestAuthorizationProvider instanceof SnowflakeManagedTokenAuthorizationProvider) {
            channelRequestAuthorizationProvider = requestAuthorizationProvider;
        } else {
            channelRequestAuthorizationProvider = new ScopedTokenAuthorizationProvider(requestAuthorizationProvider, streamingUri.getHost(), webClientService, accountUri);
        }

        final StreamingChannelClient streamingChannelClient;

        final String annotationData = context.getAnnotationData();
        if (annotationData == null || annotationData.isBlank()) {
            streamingChannelClient = new StandardStreamingChannelClient(webClientService, channelRequestAuthorizationProvider, streamingUri);
        } else {
            // Set User-Agent Tags from Processor Annotation Data to support Tracking Labels with injected flow configuration
            streamingChannelClient = new StandardStreamingChannelClient(webClientService, channelRequestAuthorizationProvider, streamingUri, annotationData);
        }

        return streamingChannelClient;
    }

    @Override
    public URI getAccountUri(final ProcessContext context) {
        final URI accountUri;

        final AuthenticationStrategy authenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(AuthenticationStrategy.class);
        if (AuthenticationStrategy.SNOWFLAKE_MANAGED == authenticationStrategy) {
            final String snowflakeHost = System.getenv(SnowflakeEnvironmentVariable.SNOWFLAKE_HOST.name());
            final String snowflakeHostUri = SNOWFLAKE_HOST_URI_FORMAT.formatted(snowflakeHost);
            accountUri = URI.create(snowflakeHostUri);
        } else {
            final String account = context.getProperty(ACCOUNT).evaluateAttributeExpressions().getValue();
            accountUri = getAccountUri(context, account);
        }

        return accountUri;
    }

    private URI getAccountUri(final ProcessContext context, final String account) {
        final String normalized = account.replace(UNDERSCORE_CHARACTER, HYPHEN_CHARACTER).toLowerCase(Locale.ROOT);
        final ConnectionStrategy connectionStrategy = context.getProperty(CONNECTION_STRATEGY).asAllowableValue(ConnectionStrategy.class);
        final String hostFormat;
        if (ConnectionStrategy.PRIVATE_CONNECTIVITY == connectionStrategy) {
            hostFormat = ACCOUNT_PRIVATE_LINK_HOST_FORMAT;
        } else {
            hostFormat = ACCOUNT_HOST_FORMAT;
        }
        final String accountHost = String.format(hostFormat, normalized);
        return URI.create(accountHost);
    }

    private URI getStreamingUri(final StreamingClient streamingClient) {
        final String streamingHostname = streamingClient.getHostname();
        return URI.create(STREAMING_URI_FORMAT.formatted(streamingHostname));
    }

    private RequestAuthorizationProvider getRequestAuthorizationProvider(final ProcessContext context) {
        final RequestAuthorizationProvider requestAuthorizationProvider;

        final AuthenticationStrategy authenticationStrategy = context.getProperty(AUTHENTICATION_STRATEGY).asAllowableValue(AuthenticationStrategy.class);
        if (AuthenticationStrategy.SNOWFLAKE_MANAGED == authenticationStrategy) {
            requestAuthorizationProvider = new SnowflakeManagedTokenAuthorizationProvider();
        } else {
            requestAuthorizationProvider = getPrivateKeyRequestAuthorizationProvider(context);
        }

        return requestAuthorizationProvider;
    }

    private RequestAuthorizationProvider getPrivateKeyRequestAuthorizationProvider(final ProcessContext context) {
        final String accountProperty = context.getProperty(ACCOUNT).evaluateAttributeExpressions().getValue();

        // Parse Account property to remove domain elements from non-production values
        final String account;
        final Matcher accountHostMatcher = HOST_PATTERN.matcher(accountProperty);
        if (accountHostMatcher.matches()) {
            account = accountHostMatcher.group(FIRST_GROUP);
        } else {
            account = accountProperty;
        }

        final String user = context.getProperty(USER).evaluateAttributeExpressions().getValue();

        final String role;
        final PropertyValue roleProperty = context.getProperty(ROLE).evaluateAttributeExpressions();
        if (roleProperty.isSet()) {
            role = roleProperty.getValue();
        } else {
            role = null;
        }

        final PrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class);
        final PrivateKey privateKey = privateKeyService.getPrivateKey();
        if (privateKey instanceof RSAPrivateCrtKey rsaPrivateKey) {
            return new RSAPrivateKeyAuthorizationProvider(account, user, role, rsaPrivateKey);
        } else {
            throw new IllegalArgumentException("Private Key algorithm [%s] not supported".formatted(privateKey.getAlgorithm()));
        }
    }

    enum VerificationStepName {
        ACCOUNT_CONFIGURATION("Account Configuration"),
        STREAMING_CONNECTION("Snowpipe Streaming Connection");

        final String label;

        VerificationStepName(final String label) {
            this.label = label;
        }
    }
}
