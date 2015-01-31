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
        // TODO it does not make sense to define this parameter on client side at all!
        String domainName = "intra.lan";
        int numberOfContainers = cluster.getSize();
        // TODO what is this variable??? Fix initialization
        int sshGroupId = 1;
        // TODO what is this variable??? Fix initialization
        int hostsGroupId = 1;
        PlacementStrategy containerPlacementStrategy = new PlacementStrategy( "ROUND_ROBIN" );

        NodeGroup clusterNodeGroup = new NodeGroup( name, templateName, domainName, numberOfContainers,
                sshGroupId, hostsGroupId, containerPlacementStrategy );


//        clusterNodeGroup.setTemplateName( cluster.getInstanceSpec().getImageId() );
//        clusterNodeGroup.setName( getClusterNodeGroupName( cluster ) );
//        clusterNodeGroup.setNumberOfNodes( cluster.getSize() );
//        clusterNodeGroup.setPlacementStrategy( new PlacementStrategy( "ROUND_ROBIN" ) );
//        clusterNodeGroup.setLinkHosts( true );
//        clusterNodeGroup.setExchangeSshKeys( true );

        return clusterNodeGroup;
    }


    public static NodeGroup getRunnerNodeGroup( final ICoordinatedStack stack, final InstanceSpec spec )
    {

        String name = getRunnersNodeGroupName( stack );
        String templateName = spec.getImageId();
        // TODO it does not make sense to define this parameter on client side at all!
        String domainName = "intra.lan";
        int numberOfContainers = stack.getRunnerCount();
        // TODO what is this variable??? Fix initialization
        int sshGroupId = 1;
        // TODO what is this variable??? Fix initialization
        int hostsGroupId = 1;
        PlacementStrategy containerPlacementStrategy = new PlacementStrategy( "ROUND_ROBIN" );

        NodeGroup runnerNodeGroup = new NodeGroup( name, templateName, domainName, numberOfContainers,
                sshGroupId, hostsGroupId, containerPlacementStrategy );


//        runnerNodeGroup.setTemplateName( spec.getImageId() );
//        runnerNodeGroup.setName( getRunnersNodeGroupName( stack ) );
//        runnerNodeGroup.setNumberOfNodes( stack.getRunnerCount() );
//        runnerNodeGroup.setPlacementStrategy( new PlacementStrategy( "ROUND_ROBIN" ) );

        return runnerNodeGroup;
    }


    public static String getClusterNodeGroupName( final Cluster cluster ){
        return cluster.getName();
    }


    public static String getRunnersNodeGroupName( final Stack stack ){
        return stack.getName() + "-runners";
    }



}
