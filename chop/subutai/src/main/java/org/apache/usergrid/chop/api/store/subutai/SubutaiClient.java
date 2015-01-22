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


import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.host.ContainerHostState;
import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.protocol.NodeGroup;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.chop.api.RestParams;
import org.apache.usergrid.chop.stack.Cluster;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.ICoordinatedStack;
import org.apache.usergrid.chop.stack.InstanceSpec;
import org.apache.usergrid.chop.stack.InstanceState;

import com.google.gson.Gson;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;


public class SubutaiClient
{
    private static final Logger LOG = LoggerFactory.getLogger( SubutaiClient.class );

    private String httpAddress;

    private static final String ENVIRONMENT_BASE_ENDPOINT = "/environment";

    public SubutaiClient( String httpAddress ) {
        this.httpAddress = httpAddress;
    }


    /**
     *
     * @param stack
     * @return the environment created for the given stack
     */
    public Environment createStackEnvironment( final ICoordinatedStack stack ) {
        // Create the blueprint from the supplied stack
        EnvironmentBlueprint blueprint = new EnvironmentBlueprint( stack.getName(),
                Common.DEFAULT_DOMAIN_NAME, true, true );
        Set<NodeGroup> clusterNodeGroups = new HashSet<NodeGroup>( stack.getClusters().size() );

        for ( Cluster cluster : stack.getClusters() ) {
            NodeGroup clusterNodeGroup = new NodeGroup();

            clusterNodeGroup.setTemplateName( cluster.getInstanceSpec().getImageId() );
            clusterNodeGroup.setName( cluster.getName() );
            clusterNodeGroup.setNumberOfNodes( cluster.getSize() );
            clusterNodeGroup.setLinkHosts( true );
            clusterNodeGroup.setExchangeSshKeys( true );

            clusterNodeGroups.add( clusterNodeGroup );
        }

        blueprint.setNodeGroups( clusterNodeGroups );


        // Send a request to build the blueprint
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentBuildResponse = resource.path( "/build" )
                                                          .queryParam( RestParams.ENVIRONMENT_BLUEPRINT, new Gson().toJson( blueprint ) )
                                                          .type( MediaType.APPLICATION_JSON )
                                                          .accept( MediaType.APPLICATION_JSON )
                                                          .post( ClientResponse.class );


//        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( "/registry" );
//        ClientResponse environmentBuildResponse = resource.path( "/templates/openjre7/2.1.1" )
//                                                          .type( MediaType.APPLICATION_JSON )
//                                                          .accept( MediaType.APPLICATION_JSON )
//                                                          .get( ClientResponse.class );
        String responseMessage = environmentBuildResponse.getEntity( String.class );
        Environment environment = new Gson().fromJson( responseMessage, Environment.class );
        return environment;
    }


    /**
     *
     * @param stack
     * @param spec
     * @return creates runner instances for the specified stack with the given instance specification
     */
    public boolean createRunnersOnEnvironment( final ICoordinatedStack stack, final InstanceSpec spec ) {
        // Get the environmentId by one of the containers in it
        ICoordinatedCluster cluster = ( ICoordinatedCluster ) stack.getClusters().get( 0 );
        if ( cluster == null ) {
            LOG.error( "Could not find any cluster set up for the {} stack", stack.getName() );
            return false;
        }
        UUID environmentId = getEnvironmentIdByInstanceId(
                UUID.fromString( cluster.getInstances().iterator().next().getId() ) );
        if ( environmentId == null ) {
            LOG.error( "Could not find environment of {} cluster", cluster.getName() );
            return false;
        }
        NodeGroup runnerNodeGroup = new NodeGroup();

        runnerNodeGroup.setTemplateName( spec.getImageId() );
        runnerNodeGroup.setName( stack.getName() + "-runners" );
        runnerNodeGroup.setNumberOfNodes( stack.getRunnerCount() );

        // Send a request to add the nodegroup the specified environment
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse addNodeGroupResponse = resource.path( "/add" )
                                                      .queryParam( RestParams.ENVIRONMENT_ID, environmentId.toString() )
                                                      .queryParam( RestParams.NODE_GROUP, new Gson().toJson( runnerNodeGroup ) )
                                                      .type( MediaType.APPLICATION_JSON )
                                                      .post( ClientResponse.class );

        if( addNodeGroupResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            LOG.error( "Could not create runner instances on {} environment, HTTP status: {}",
                    getEnvironmentByEnvironmentId( environmentId ).getName(), addNodeGroupResponse.getStatus() );
            return false;
        }

        return true;
    }


    /**
     *
     * @param instanceId
     * @return the environmentId for the given instance
     */
    public UUID getEnvironmentIdByInstanceId( UUID instanceId ) {
        // Send a request to add the nodegroup the specified environment
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentIdResponse = resource.path( "/getEnvironmentId" )
                                                      .queryParam( RestParams.INSTANCE_ID, instanceId.toString() )
                                                      .type( MediaType.APPLICATION_JSON )
                                                      .post( ClientResponse.class );

        String responseMessage = environmentIdResponse.getEntity( String.class );
        UUID environmentId = UUID.fromString( responseMessage );
        return environmentId;
    }


    /**
     *
     * @param environmentId
     * @return the environment for the given environmentId
     */
    public Environment getEnvironmentByEnvironmentId( UUID environmentId ) {
        // Send a request to build the blueprint
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentGetResponse = resource.path( "/getEnvironment" )
                                                        .queryParam( RestParams.ENVIRONMENT_ID, environmentId.toString() )
                                                        .type( MediaType.APPLICATION_JSON )
                                                        .accept( MediaType.APPLICATION_JSON )
                                                        .post( ClientResponse.class );

        String responseMessage = environmentGetResponse.getEntity( String.class );
        Environment environment = new Gson().fromJson( responseMessage, Environment.class );
        return environment;
    }


    public boolean destroyEnvironment( UUID environmentId ) {
        // Send a request to build the blueprint
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentDestroyResponse = resource.path( "/destroy" )
                                                        .queryParam( RestParams.ENVIRONMENT_ID,
                                                                environmentId.toString() )
                                                        .type( MediaType.APPLICATION_JSON )
                                                        .delete( ClientResponse.class );

        boolean success = environmentDestroyResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        return success;
    }


    public boolean destroyInstance( UUID instanceId ) {
        // Send a request to build the blueprint
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse instanceDestroyResponse = resource.path( "/destroy" )
                                                            .queryParam( RestParams.INSTANCE_ID, instanceId.toString() )
                                                            .type( MediaType.APPLICATION_JSON )
                                                            .delete( ClientResponse.class );

        boolean success = instanceDestroyResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        return success;
    }


    public InstanceState getInstanceState( UUID instanceId ) {
        // Send a request to build the blueprint
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( "http://" + httpAddress + "/cxf" ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse instanceDestroyResponse = resource.path( "/getContainerState" )
                                                         .queryParam( RestParams.INSTANCE_ID, instanceId.toString() )
                                                         .type( MediaType.APPLICATION_JSON )
                                                         .delete( ClientResponse.class );

        String responseMessage = instanceDestroyResponse.getEntity( String.class );
        ContainerHostState containerHostState = new Gson().fromJson( responseMessage, ContainerHostState.class );
        InstanceState instanceState = InstanceState.fromContainerHostState( containerHostState );
        return instanceState;
    }


    /**
     *
     * @param stack
     * @param cluster
     * @return true if the cluster is succesfully configured
     */
    public boolean configureCluster( final ICoordinatedStack stack, final Cluster cluster ) {
        // TODO implement this functionality
        return true;
    }

}
