/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.usergrid.chop.webapp;


import org.safehaus.guicyfig.Default;
import org.safehaus.guicyfig.FigSingleton;
import org.safehaus.guicyfig.GuicyFig;
import org.safehaus.guicyfig.Key;

import org.apache.usergrid.chop.api.Constants;


/**
 * Servlet configuration information.
 */
@FigSingleton
public interface ChopUiFig extends GuicyFig {
    String CONTEXT_PATH = "context.path";

    @Key(ChopUiFig.CONTEXT_PATH)
    String getContextPath();


    String SERVER_INFO_KEY = "server.info";

    @Key(ChopUiFig.SERVER_INFO_KEY)
    String getServerInfo();


    String CONTEXT_TEMPDIR_KEY = "javax.servlet.context.tempdir";

    @Key(ChopUiFig.CONTEXT_TEMPDIR_KEY)
    String getContextTempDir();


    /**
     * prop key for number of times to retry recovery operations
     */
    String RECOVERY_RETRY_COUNT_KEY = "recovery.retry.count";
    /**
     * default for number of times to retry recovery operations
     */
    String DEFAULT_RECOVERY_RETRY_COUNT = "3";

    /**
     * Gets the number of times to retry recovery operations. Uses {@link
     * ChopUiFig#RECOVERY_RETRY_COUNT_KEY} to access the retry count.
     *
     * @return the number of retries for recovery
     */
    @Default(ChopUiFig.DEFAULT_RECOVERY_RETRY_COUNT)
    @Key(ChopUiFig.RECOVERY_RETRY_COUNT_KEY)
    int getRecoveryRetryCount();


    /**
     * prop key for the time to wait between retry recovery operations
     */
    String DELAY_RETRY_KEY = "recovery.retry.delay";
    /**
     * default for the time to wait in milliseconds between retry recovery operations
     */
    String DEFAULT_DELAY_RETRY = "10000";

    /**
     * Gets the amount of time to wait between retry operations. Uses {@link
     * ChopUiFig#DELAY_RETRY_KEY} to access the recovery delay.
     *
     * @return the time in milliseconds to delay between retry operations
     */
    @Default(ChopUiFig.DEFAULT_DELAY_RETRY)
    @Key(ChopUiFig.DELAY_RETRY_KEY)
    long getRetryDelay();


    String LAUNCH_CLUSTER_TIMEOUT_KEY = "launch.cluster.timeout";
    String DEFAULT_LAUNCH_CLUSTER_TIMEOUT = "100000";

    /**
     * Gets the timeout value for launching both clusters and runner instances
     *
     * @return The maximum time in milliseconds to wait for clusters to come up
     */
    @Default(ChopUiFig.DEFAULT_LAUNCH_CLUSTER_TIMEOUT)
    @Key(ChopUiFig.LAUNCH_CLUSTER_TIMEOUT_KEY)
    int getLaunchClusterTimeout();


    String SERVICE_PROVIDER_KEY = "service.provider";
    String DEFAULT_SERVICE_PROVIDER = Constants.AMAZON_PROVIDER_NAME;

    /**
     *
     * @return The service provider used in the webapp
     */
    @Default(ChopUiFig.DEFAULT_SERVICE_PROVIDER)
    @Key(ChopUiFig.SERVICE_PROVIDER_KEY)
    String getServiceProvider();
}
