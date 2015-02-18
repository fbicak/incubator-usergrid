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


import org.apache.usergrid.chop.stack.BasicInstance;
import org.apache.usergrid.chop.stack.BasicInstanceSpec;
import org.apache.usergrid.chop.stack.Cluster;
import org.apache.usergrid.chop.stack.ICoordinatedStack;
import org.apache.usergrid.chop.stack.Instance;
import org.apache.usergrid.chop.stack.InstanceSpec;
import org.apache.usergrid.chop.stack.InstanceState;
import org.apache.usergrid.chop.stack.Stack;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.host.ContainerHostState;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.core.env.rest.ContainerJson;


public class SubutaiUtils
{


    public static Instance getInstanceFromContainer( ContainerJson containerHost ) {
        BasicInstanceSpec instanceSpec = new BasicInstanceSpec();
        instanceSpec.setImageId( containerHost.getTemplateName() );

        String privateIpAddress = containerHost.getIp();
        String publicIpAddress = containerHost.getIp();
        String privateDnsName = containerHost.getIp();
        String publicDnsName = containerHost.getIp();

        ContainerHostState containerState;
        containerState = containerHost.getState();
        InstanceState instanceState = InstanceState.fromContainerHostState( containerState );
        Instance instance = new BasicInstance( containerHost.getId().toString(), instanceSpec, instanceState,
                privateDnsName, publicDnsName, privateIpAddress, publicIpAddress );
        return instance;
    }


    public static NodeGroup getClusterNodeGroup( final Cluster cluster )
    {
        String name = getClusterNodeGroupName( cluster );
        String templateName = cluster.getInstanceSpec().getImageId();
        int numberOfContainers = cluster.getSize();
        int sshGroupId = getClusterGroupId( cluster );
        int hostsGroupId = getClusterGroupId( cluster );
        PlacementStrategy containerPlacementStrategy = new PlacementStrategy( "ROUND_ROBIN" );

        NodeGroup clusterNodeGroup = new NodeGroup( name, templateName, numberOfContainers,
                sshGroupId, hostsGroupId, containerPlacementStrategy );

        return clusterNodeGroup;
    }


    public static NodeGroup getRunnerNodeGroup( final ICoordinatedStack stack, final InstanceSpec spec )
    {
        String name = getRunnersNodeGroupName( stack );
        String templateName = spec.getImageId();
        int numberOfContainers = stack.getRunnerCount();
        int sshGroupId = getRunnerGroupId( stack );
        int hostsGroupId = getRunnerGroupId( stack );
        PlacementStrategy containerPlacementStrategy = new PlacementStrategy( "ROUND_ROBIN" );

        NodeGroup runnerNodeGroup = new NodeGroup( name, templateName, numberOfContainers,
                sshGroupId, hostsGroupId, containerPlacementStrategy );

        return runnerNodeGroup;
    }


    public static SubutaiPlugin getSubutaiPluginFromString( final String configuratorPlugin )
    {
        SubutaiPlugin plugin = null;

        if ( configuratorPlugin.toLowerCase().equals( "cassandra" ) ) {
            plugin = SubutaiPlugin.Cassandra;
        }
        else if ( configuratorPlugin.toLowerCase().equals( "hadoop" ) ) {
            plugin = SubutaiPlugin.Hadoop;
        }

        return plugin;
    }


    public static String getClusterNodeGroupName( final Cluster cluster ){
        return cluster.getName();
    }


    public static String getRunnersNodeGroupName( final Stack stack ){
        return stack.getName() + "-runners";
    }


    private static int getClusterGroupId( Cluster cluster ) {
        return 1;
    }


    private static int getRunnerGroupId( ICoordinatedStack stack ) {
        return 1;
    }


    public static String getPublicKeyFileName( String privateKeyName ) {
        return privateKeyName + ".pub";
    }
}
