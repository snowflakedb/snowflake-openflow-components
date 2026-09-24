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

import net.snowflake.openflow.processors.snowpipe.streaming.property.DestinationType;
import org.apache.nifi.migration.PropertyConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamingDestinationProviderTest {

    @Mock
    private PropertyConfiguration propertyConfiguration;

    static final List<Arguments> DEFAULT_PIPE_ARGUMENTS = List.of(
        Arguments.of("SNOWPIPE-STREAMING", "SNOWPIPE"),
        Arguments.of("\"SNOWPIPE-STREAMING\"", "\"SNOWPIPE\"")
    );

    @ParameterizedTest
    @FieldSource("DEFAULT_PIPE_ARGUMENTS")
    void testMigratePropertiesDefaultPipe(final String pipe, final String expectedTable) {
        when(propertyConfiguration.getPropertyValue(StreamingDestinationProvider.DESTINATION_TYPE)).thenReturn(Optional.empty());
        when(propertyConfiguration.getRawPropertyValue(StreamingDestinationProvider.PIPE)).thenReturn(Optional.of(pipe));

        StreamingDestinationProvider.migrateProperties(propertyConfiguration);

        verify(propertyConfiguration).setProperty(StreamingDestinationProvider.TABLE, expectedTable);
        verify(propertyConfiguration).setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.TABLE.getValue());
    }

    @Test
    void testMigratePropertiesStandardPipe() {
        when(propertyConfiguration.getPropertyValue(StreamingDestinationProvider.DESTINATION_TYPE)).thenReturn(Optional.empty());
        when(propertyConfiguration.getRawPropertyValue(StreamingDestinationProvider.PIPE)).thenReturn(Optional.of("STANDARD_PIPE"));

        StreamingDestinationProvider.migrateProperties(propertyConfiguration);

        verify(propertyConfiguration).setProperty(StreamingDestinationProvider.DESTINATION_TYPE, DestinationType.PIPE.getValue());
    }

    @Test
    void testMigratePropertiesDestinationTypeConfigured() {
        when(propertyConfiguration.getPropertyValue(StreamingDestinationProvider.DESTINATION_TYPE)).thenReturn(Optional.of(DestinationType.TABLE.getValue()));

        StreamingDestinationProvider.migrateProperties(propertyConfiguration);

        verify(propertyConfiguration).getPropertyValue(StreamingDestinationProvider.DESTINATION_TYPE);
        verifyNoMoreInteractions(propertyConfiguration);
    }
}
