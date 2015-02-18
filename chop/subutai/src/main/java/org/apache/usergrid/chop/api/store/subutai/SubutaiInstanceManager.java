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
package org.apache.usergrid.chop.api.store.subutai;


import com.google.inject.Inject;
import org.apache.usergrid.chop.spi.InstanceManager;
import org.apache.usergrid.chop.spi.LaunchResult;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.ICoordinatedStack;
import org.apache.usergrid.chop.stack.Instance;
import org.apache.usergrid.chop.stack.InstanceSpec;
import org.apache.usergrid.chop.stack.InstanceState;
import org.safehaus.subutai.core.env.rest.ContainerJson;
import org.safehaus.subutai.core.env.rest.EnvironmentJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;


public class SubutaiInstanceManager implements InstanceManager
{

    private static Logger LOG = LoggerFactory.getLogger( SubutaiInstanceManager.class );

    private static final long SLEEP_LENGTH = 3000;

    private SubutaiClient subutaiClient;


    /**
     * @param subutaiFig Fig object containing Subutai peer information
     */
    @Inject
    public SubutaiInstanceManager( SubutaiFig subutaiFig ) {
        subutaiClient = new SubutaiClient( subutaiFig.getSubutaiPeerSite() );
    }


    @Override
    public long getDefaultTimeout()
    {
        return SLEEP_LENGTH;
    }


    @Override
    public void setDataCenter( final String dataCenter )
    {
        subutaiClient.setHttpAddress( dataCenter );
    }


    @Override
    public void terminateInstances( final Collection<String> instanceIds )
    {
        if ( instanceIds.size() == 0 ) {
            LOG.info( "No instance found to be terminated" );
            return;
        }

        UUID environmentId = subutaiClient
                .getEnvironmentIdByInstanceId( UUID.fromString( instanceIds.iterator().next() ) );

        EnvironmentJson environment =
                subutaiClient.getEnvironmentByEnvironmentId( environmentId );

        // Destroy the environment if all the instances are inside the same environment
        // and there is no other instances inside the environment than the provided ones
        if ( instanceIds.size() == environment.getContainers().size() ) {
            LOG.debug( "Checking if all the instances are in the same environment" );
            int matchCount = 0;
            for ( String instanceId : instanceIds ) {
                for ( ContainerJson container : environment.getContainers() ) {
                    if ( instanceId.equals( container.getId().toString() ) ) {
                        matchCount++;
                        break;
                    }
                }
            }

            if ( matchCount == instanceIds.size() ) {
                LOG.info( "Environment {} does not contain other instances than provided instances to be terminated. " +
                        "Therefore, destroying the environment completely...", environment.getName() );
                subutaiClient.destroyEnvironment( environmentId );
                return;
            }
            LOG.debug( "Not all the instances are in the same environment, so destroying only provided instances." );
        }

        for ( String instanceId : instanceIds ) {
            environmentId = subutaiClient
                    .getEnvironmentIdByInstanceId( UUID.fromString( instanceIds.iterator().next() ) );
            subutaiClient.destroyInstanceByInstanceId( UUID.fromString( instanceId ) );
        }

        // Destroy environment if no instance left inside
        if( environmentId == null ) {
            LOG.warn( "Could not find the environment that instance(id:{}) belongs to, " +
                    "and could not check if there is any instance left on the environment. " +
                    "Therefore not destroying the environment if environment does not have instance left!" );
            return;
        }
        environment = subutaiClient.getEnvironmentByEnvironmentId( environmentId );
        if ( environment == null ) {
            LOG.warn( "Could not find environment({}), not destroying the environment!", environmentId );
            return;
        }

        if ( environment.getContainers().size() == 0 ) {
            LOG.info( "Destroying environment {} since no instance left in it.",
                    environment.getName()+"(" + environment.getId() + ")" );
            subutaiClient.destroyEnvironment( environmentId );
        }

    }


    @Override
    public LaunchResult launchCluster( final ICoordinatedStack stack, final ICoordinatedCluster cluster,
                                       final int timeout, final String publicKeyFilePath )
    {
        LOG.info( "Creating the cluster instances on {}", stack.getDataCenter() );

        List<String> instanceIds = new ArrayList<String>( cluster.getSize() );
        Collection<Instance> instances = new ArrayList<Instance>();
        // Create the environment
        if ( publicKeyFilePath == null ) {
            LOG.error( "Public key file path cannot be null!" );
            return new SubutaiLaunchResult( cluster.getInstanceSpec(), Collections.EMPTY_LIST );
        }
        File publicKeyFile = new File( publicKeyFilePath );
        if ( ! publicKeyFile.exists() ) {
            LOG.error( "Public key {} does not exists!", publicKeyFilePath );
            return new SubutaiLaunchResult( cluster.getInstanceSpec(), Collections.EMPTY_LIST );
        }
        EnvironmentJson environment = subutaiClient.createStackEnvironment( stack, publicKeyFile );

        if ( environment == null ) {
            LOG.error( "Could not create environment for {} stack!", stack.getName() );
            return new SubutaiLaunchResult( cluster.getInstanceSpec(), Collections.EMPTY_LIST );
        }

        Set<ContainerJson> containerHosts = environment.getContainers();

        Iterator<ContainerJson> iterator = containerHosts.iterator();

        while( iterator.hasNext() ) {
            ContainerJson containerHost = iterator.next();
            Instance instance = SubutaiUtils.getInstanceFromContainer( containerHost );
            instances.add( instance );
        }

        if ( timeout > SLEEP_LENGTH ) {
            LOG.info( "Waiting for maximum {} msec until all instances are running", timeout );

            boolean stateCheck = waitUntil( instanceIds, InstanceState.Running, timeout );

            if ( ! stateCheck ) {
                LOG.warn( "Waiting for instances to get into Running state has timed out" );
            }
            else {
                LOG.info( "All the cluster instances are running" );
            }
        }

        if ( cluster.getConfiguratorPlugin() != null ) {
            boolean isConfigurationSuccessful = subutaiClient.configureCluster( cluster, instances );

            if ( ! isConfigurationSuccessful ) {
                LOG.error( "Configuration of {} cluster failed with {} plugin! Destroying environment...",
                        cluster.getName(), cluster.getConfiguratorPlugin() );
                subutaiClient.destroyEnvironment( environment.getId() );
                return new SubutaiLaunchResult( cluster.getInstanceSpec(), Collections.EMPTY_LIST );
            }
        }
        else {
            LOG.warn( "No configurator plugin defined in stack.json, skipping configuration of {}", cluster.getName() );
        }

        return new SubutaiLaunchResult( cluster.getInstanceSpec(), instances );
    }


    @Override
    public LaunchResult launchRunners( final ICoordinatedStack stack, final InstanceSpec spec, final int timeout )
    {
        LOG.info( "Creating the runner instances on {}", stack.getDataCenter() );

        List<String> instanceIds = new ArrayList<String>( stack.getRunnerCount() );
        Collection<Instance> runnerInstances = subutaiClient.createRunnersOnEnvironment( stack, spec );
        Iterator<Instance> iterator = runnerInstances.iterator();

        while( iterator.hasNext() ) {
            Instance instance = iterator.next();
            runnerInstances.add( instance );
        }

        if ( runnerInstances.size() == 0 ) {
            LOG.error( "Runner runnerInstances could not created for {} stack with {} image!", stack.getName(),
                    spec.getImageId() );
            return new SubutaiLaunchResult( spec, Collections.EMPTY_LIST );

        }
        else if ( runnerInstances.size() != stack.getRunnerCount() ) {
            LOG.error( "%s runner instances created out of %s! Terminating runner instances", runnerInstances.size(), stack.getRunnerCount() );

            while( iterator.hasNext() ) {
                Instance instance = iterator.next();
                subutaiClient.destroyInstanceByInstanceId( UUID.fromString( instance.getId() ) );
            }
            return new SubutaiLaunchResult( spec, Collections.EMPTY_LIST );
        }


        if ( timeout > SLEEP_LENGTH ) {
            LOG.info( "Waiting for maximum {} msec until all runner instances are running", timeout );
            boolean stateCheck = waitUntil( instanceIds, InstanceState.Running, timeout );

            if ( ! stateCheck ) {
                LOG.warn( "Waiting for runner instances to get into Running state has timed out" );
            }
        }

        LOG.info( "All the runner runner instances are running" );

        return new SubutaiLaunchResult( spec, runnerInstances );
    }


    @Override
    public Collection<Instance> getClusterInstances( final ICoordinatedStack stack, final ICoordinatedCluster cluster )
    {
        // TODO implement this functionality
        LOG.warn( "This method is not implemented" );
        return new ArrayList<Instance>( 0 );
    }


    @Override
    public Collection<Instance> getRunnerInstances( final ICoordinatedStack stack )
    {
        // TODO implement this functionality
        LOG.warn( "This method is not implemented" );
        return new ArrayList<Instance>( 0 );
    }


    public boolean waitUntil ( Collection<String> instanceIds, InstanceState state,  int timeout ) {
        // TODO implement this functionality
        LOG.warn( "This method is not implemented" );
        return true;
    }

}
