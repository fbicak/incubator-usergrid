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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.host.ContainerHostState;
import org.safehaus.subutai.core.environment.rest.ContainerJson;
import org.safehaus.subutai.core.environment.rest.EnvironmentJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.chop.spi.InstanceManager;
import org.apache.usergrid.chop.spi.LaunchResult;
import org.apache.usergrid.chop.stack.BasicInstance;
import org.apache.usergrid.chop.stack.BasicInstanceSpec;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.ICoordinatedStack;
import org.apache.usergrid.chop.stack.Instance;
import org.apache.usergrid.chop.stack.InstanceSpec;
import org.apache.usergrid.chop.stack.InstanceState;

import com.google.inject.Inject;


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
        UUID environmentId =
                subutaiClient.getEnvironmentIdByInstanceId( UUID.fromString( instanceIds.iterator().next() ) );
        for ( String instanceId : instanceIds ) {
            subutaiClient.destroyInstanceByInstanceId( UUID.fromString( instanceId ) );
        }

        // Destroy environment if no instance left inside
        EnvironmentJson environment =
                subutaiClient.getEnvironmentByEnvironmentId( environmentId );
        if ( environment.getContainers().size() == 0 ) {
            subutaiClient.destroyEnvironment( environmentId );
        }

    }


    @Override
    public LaunchResult launchCluster( final ICoordinatedStack stack, final ICoordinatedCluster cluster,
                                       final int timeout )
    {
        LOG.info( "Creating the cluster instances on {}", stack.getDataCenter() );

        List<String> instanceIds = new ArrayList<String>( cluster.getSize() );
        Collection<Instance> instances = new ArrayList<Instance>();
        EnvironmentJson environment = subutaiClient.createStackEnvironment( stack );
        Set<ContainerJson> containerHosts = environment.getContainers();

        Iterator<ContainerJson> iterator = containerHosts.iterator();

        while( iterator.hasNext() ) {
            ContainerJson containerHost = iterator.next();
            BasicInstanceSpec instanceSpec = new BasicInstanceSpec();
            instanceSpec.setImageId( containerHost.getTemplateName() );

            String privateIpAddress = containerHost.getIp();
            String publicIpAddress = containerHost.getIp();
            String privateDnsName = containerHost.getHostname();
            String publicDnsName = containerHost.getHostname();

            ContainerHostState containerState;
            containerState = containerHost.getState();
            InstanceState instanceState = InstanceState.fromContainerHostState( containerState );
            Instance instance = new BasicInstance( containerHost.getId().toString(), instanceSpec, instanceState,
                    privateDnsName, publicDnsName, privateIpAddress, publicIpAddress );
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

        LOG.info( "Configuring the cluster {}", cluster.getName() );

        boolean isConfigurationSuccessful = subutaiClient.configureCluster( stack, cluster );

        if ( isConfigurationSuccessful )
            LOG.info( "Cluster {} configured ", cluster.getName() );
        else {
            LOG.error( "Configuration of {} cluster failed!", cluster.getName() );
            return new SubutaiLaunchResult( cluster.getInstanceSpec(), instances );
        }

        return new SubutaiLaunchResult( cluster.getInstanceSpec(), instances );
    }


    @Override
    public LaunchResult launchRunners( final ICoordinatedStack stack, final InstanceSpec spec, final int timeout )
    {
        LOG.info( "Creating the runner instances on {}", stack.getDataCenter() );

        List<String> instanceIds = new ArrayList<String>( stack.getRunnerCount() );


        if ( timeout > SLEEP_LENGTH ) {
            LOG.info( "Waiting for maximum {} msec until all instances are running", timeout );
            boolean stateCheck = waitUntil( instanceIds, InstanceState.Running, timeout );

            if ( ! stateCheck ) {
                LOG.warn( "Waiting for instances to get into Running state has timed out" );
            }
        }

        LOG.info( "All the runner instances are running" );

        Collection<Instance> instances = new ArrayList<Instance>( 0 );

        return new SubutaiLaunchResult( spec, instances );
    }


    @Override
    public Collection<Instance> getClusterInstances( final ICoordinatedStack stack, final ICoordinatedCluster cluster )
    {
        // TODO implement this functionality
        return new ArrayList<Instance>( 0 );
    }


    @Override
    public Collection<Instance> getRunnerInstances( final ICoordinatedStack stack )
    {
        // TODO implement this functionality
        return new ArrayList<Instance>( 0 );
    }


    public boolean waitUntil ( Collection<String> instanceIds, InstanceState state,  int timeout ) {
        // TODO implement this functionality
        return true;
    }

}
