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

package net.snowflake.openflow.components.snowpipe.streaming;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

/**
 * Provider for HTTP User-Agent request header containing application version and platform information
 */
public class UserAgentProvider {

    private static final String USER_AGENT_FORMAT = "%s/%s (%s) JAVA/%s";

    private static final String USER_AGENT_PRODUCT = "SnowflakeOpenflowUnmanaged";

    private static final String USER_AGENT_VERSION = "SNAPSHOT";

    private static final String PLATFORM_UNKNOWN = "UNKNOWN";

    private static final String PLATFORM_FORMAT = "%s %s %s";

    private static final String USER_AGENT;

    static {
        final Package providerPackage = UserAgentProvider.class.getPackage();
        final String userAgentVersion;
        if (providerPackage == null || providerPackage.getImplementationVersion() == null) {
            userAgentVersion = USER_AGENT_VERSION;
        } else {
            // Set User Agent Version from JAR Manifest when found
            userAgentVersion = providerPackage.getImplementationVersion();
        }

        final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        final String platform;
        if (operatingSystemMXBean == null) {
            platform = PLATFORM_UNKNOWN;
        } else {
            final String operatingSystemName = operatingSystemMXBean.getName();
            final String operatingSystemVersion = operatingSystemMXBean.getVersion();
            final String operatingSystemArch = operatingSystemMXBean.getArch();
            platform = PLATFORM_FORMAT.formatted(operatingSystemName, operatingSystemVersion, operatingSystemArch);
        }

        final Runtime.Version runtimeVersion = Runtime.version();
        USER_AGENT = USER_AGENT_FORMAT.formatted(USER_AGENT_PRODUCT, userAgentVersion, platform, runtimeVersion);
    }

    public static String getUserAgent() {
        return USER_AGENT;
    }
}
