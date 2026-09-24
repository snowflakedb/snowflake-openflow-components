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

import net.snowflake.openflow.processors.snowpipe.streaming.channel.StreamingDestination;
import net.snowflake.openflow.processors.snowpipe.streaming.property.ChannelType;
import net.snowflake.openflow.processors.snowpipe.streaming.property.DestinationType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Streaming Destination Provider with standard properties for Processor configuration
 */
class StreamingDestinationProvider {

    static final PropertyDescriptor DESTINATION_TYPE = new PropertyDescriptor.Builder()
            .name("Destination Type")
            .description("Snowflake destination object for processed records with support for derived Default Pipes")
            .required(true)
            .defaultValue(DestinationType.TABLE)
            .allowableValues(DestinationType.class)
            .build();

    static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("Database")
            .description("Snowflake Database destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("Schema")
            .description("Snowflake Schema destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor PIPE = new PropertyDescriptor.Builder()
            .name("Pipe")
            .description("Snowflake Pipe destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(DESTINATION_TYPE, DestinationType.PIPE)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table")
            .description("Snowflake Table destination for processed records")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .dependsOn(DESTINATION_TYPE, DestinationType.TABLE)
            .build();

    static final PropertyDescriptor CHANNEL_TYPE = new PropertyDescriptor.Builder()
            .name("Channel Type")
            .description("Channel configuration strategy for Snowpipe Streaming")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .defaultValue(ChannelType.STANDARD)
            .allowableValues(ChannelType.class)
            .build();

    static final PropertyDescriptor CHANNEL_GROUP = new PropertyDescriptor.Builder()
            .name("Channel Group")
            .description("Group for managing distinct Snowpipe Streaming Channels with partitioning")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue("SHARED")
            .required(true)
            .dependsOn(CHANNEL_TYPE, ChannelType.STANDARD)
            .build();

    /** Default Pipe defined according to Snowpipe Streaming High-Performance Architecture released on 2025-12-11 */
    private static final String DEFAULT_PIPE_SUFFIX = "-STREAMING";

    private static final Pattern DEFAULT_PIPE_PATTERN = Pattern.compile("(.+?)-STREAMING\"?");

    private static final int FIRST_GROUP = 1;

    private static final char DOUBLE_QUOTE = '"';

    static void migrateProperties(final PropertyConfiguration propertyConfiguration) {
        final Optional<String> destinationTypeProperty = propertyConfiguration.getPropertyValue(DESTINATION_TYPE);
        if (destinationTypeProperty.isEmpty()) {
            // Set Destination Type when Pipe is configured
            final Optional<String> pipePropertyFound = propertyConfiguration.getRawPropertyValue(PIPE);
            if (pipePropertyFound.isPresent()) {
                final String pipeProperty = pipePropertyFound.get();
                final Matcher defaultPipeMatcher = DEFAULT_PIPE_PATTERN.matcher(pipeProperty);
                if (defaultPipeMatcher.matches()) {
                    final String table = getTable(defaultPipeMatcher);
                    propertyConfiguration.setProperty(TABLE, table);
                    propertyConfiguration.setProperty(DESTINATION_TYPE, DestinationType.TABLE.getValue());
                } else {
                    propertyConfiguration.setProperty(DESTINATION_TYPE, DestinationType.PIPE.getValue());
                }
            }
        }
    }

    StreamingDestination getStreamingDestination(final ProcessContext context, final FlowFile flowFile) {
        final String database = context.getProperty(DATABASE).evaluateAttributeExpressions(flowFile).getValue();
        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();

        final DestinationType destinationType = context.getProperty(DESTINATION_TYPE).asAllowableValue(DestinationType.class);

        final String pipe;
        if (DestinationType.TABLE == destinationType) {
            final String table = context.getProperty(TABLE).evaluateAttributeExpressions(flowFile).getValue();
            pipe = getDefaultPipe(table);
        } else {
            pipe = context.getProperty(PIPE).evaluateAttributeExpressions(flowFile).getValue();
        }

        final ChannelType channelType = context.getProperty(CHANNEL_TYPE).asAllowableValue(ChannelType.class);
        final String channelGroup;

        if (ChannelType.ELASTIC == channelType) {
            // ELASTIC is the reserved word indicating use of Elastic Channels
            channelGroup = ChannelType.ELASTIC.getValue();
        } else {
            channelGroup = context.getProperty(CHANNEL_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        }

        return new StreamingDestination(database, schema, pipe, channelGroup);
    }

    private static String getTable(final Matcher defaultPipeMatcher) {
        final String tableGroup = defaultPipeMatcher.group(FIRST_GROUP);

        final String pipe = defaultPipeMatcher.group();
        final int lastCharacterIndex = pipe.length() - 1;
        final char lastCharacter = pipe.charAt(lastCharacterIndex);

        final String table;
        if (DOUBLE_QUOTE == lastCharacter) {
            // Append double quote character when found on Pipe property
            table = tableGroup + DOUBLE_QUOTE;
        } else {
            table = tableGroup;
        }

        return table;
    }

    private String getDefaultPipe(final String table) {
        final String pipe;

        if (table == null || table.isEmpty()) {
            throw new IllegalStateException("Default Pipe not resolved: Table not found");
        }

        final int lastCharacterIndex = table.length() - 1;
        final char lastCharacter = table.charAt(lastCharacterIndex);
        if (DOUBLE_QUOTE == lastCharacter) {
            final String tablePrefix = table.substring(0, lastCharacterIndex);
            pipe = tablePrefix + DEFAULT_PIPE_SUFFIX + DOUBLE_QUOTE;
        } else {
            pipe = table + DEFAULT_PIPE_SUFFIX;
        }
        return pipe;
    }
}
