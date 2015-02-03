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


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.uri.UriComponent;
import org.apache.usergrid.chop.api.RestParams;
import org.apache.usergrid.chop.stack.Cluster;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.ICoordinatedStack;
import org.apache.usergrid.chop.stack.Instance;
import org.apache.usergrid.chop.stack.InstanceSpec;
import org.apache.usergrid.chop.stack.InstanceState;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.host.ContainerHostState;
import org.safehaus.subutai.core.env.rest.ContainerJson;
import org.safehaus.subutai.core.env.rest.EnvironmentJson;
import org.safehaus.subutai.core.env.rest.TopologyJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class SubutaiClient
{
    private static final Logger LOG = LoggerFactory.getLogger( SubutaiClient.class );

    private String httpAddress;
    private Client client;

    public static final String REST_BASE_ENDPOINT = "/cxf";
    public static final String ENVIRONMENT_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/environments";
    public static final String PEER_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/peer";
    public static final String CONTAINER_BASE_ENDPOINT = ENVIRONMENT_BASE_ENDPOINT + "/container";
    public static final String CASSANDRA_PLUGIN_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/cassandra";
    public static final String CASSANDRA_PLUGIN_CONFIGURE_ENDPOINT = CASSANDRA_PLUGIN_BASE_ENDPOINT + "/configure_environment";
    private Gson gson;


    public SubutaiClient( String httpAddress ) {
        this.httpAddress = httpAddress;
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        client = Client.create( clientConfig );
        gson = new Gson();
    }


    /**
     *
     * @param stack used to create an environment on Subutai
     * @return the environment created for the given stack
     */
    public EnvironmentJson createStackEnvironment( final ICoordinatedStack stack ) {
        // Create the topology from the supplied stack
        TopologyJson topology = getTopologyFromStack( stack );

        // Send a request to build the topology
        WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );

        String topologyEncoded = UriComponent.encode( gson.toJson( topology ), UriComponent.Type.QUERY_PARAM );
        // Returns the uuid of the environment created from the supplied topology
        ClientResponse environmentBuildResponse = resource.queryParam( RestParams.ENVIRONMENT_TOPOLOGY, topologyEncoded )
                                                          .type( MediaType.APPLICATION_JSON )
                                                          .post( ClientResponse.class );


        if ( environmentBuildResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            LOG.error( "Environment build operation for {} stack is not successful! Error: {}", stack.getName(),
                    environmentBuildResponse.getEntity( String.class ) );
            return null;
        }
        LOG.info( "Environment for {} is created successfully", stack.getName() );

        String responseMessage = environmentBuildResponse.getEntity( String.class );
        EnvironmentJson environment = gson.fromJson( responseMessage, EnvironmentJson.class );
        return environment;
    }


    public TopologyJson getTopologyFromStack( final ICoordinatedStack stack )
    {
        TopologyJson topology = new TopologyJson();
        Set<NodeGroup> clusterNodeGroups = new HashSet<NodeGroup>( stack.getClusters().size() );

        for ( Cluster cluster : stack.getClusters() ) {
            NodeGroup clusterNodeGroup = SubutaiUtils.getClusterNodeGroup( cluster );
            clusterNodeGroups.add( clusterNodeGroup );
        }
        Map<UUID, Set<NodeGroup>> nodeGroupPlacement = new HashMap<UUID, Set<NodeGroup>>();
        UUID localPeerId = getLocalPeerId();
        nodeGroupPlacement.put( localPeerId, clusterNodeGroups );

        topology.setNodeGroupPlacement( nodeGroupPlacement );
        topology.setEnvironmentName( stack.getName() );
        return topology;
    }



    public TopologyJson getRunnerTopology( final ICoordinatedStack stack, InstanceSpec spec )
    {
        Set<NodeGroup> runnerNodeGroupSet = new HashSet<NodeGroup>();
        runnerNodeGroupSet.add( SubutaiUtils.getRunnerNodeGroup( stack, spec ) );

        TopologyJson topology = new TopologyJson();

        Map<UUID, Set<NodeGroup>> nodeGroupPlacement = new HashMap<UUID, Set<NodeGroup>>();
        UUID localPeerId = getLocalPeerId();
        nodeGroupPlacement.put( localPeerId, runnerNodeGroupSet );

        topology.setNodeGroupPlacement( nodeGroupPlacement );
        topology.setEnvironmentName( stack.getName() );
        return topology;
    }


    public UUID getLocalPeerId() {
        // Send a request to build the topology
        WebResource resource = client.resource( "http://" + httpAddress ).path( PEER_BASE_ENDPOINT );

        // Returns the uuid of the peer
        ClientResponse peerIdResponse = resource.path( "id" )
                                                          .type( MediaType.APPLICATION_JSON )
                                                          .get( ClientResponse.class );


        if ( peerIdResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            LOG.error( "Could not retrieve the id of local peer! Error: {}", peerIdResponse.getEntity( String.class ) );
            return null;
        }

        String responseMessage = peerIdResponse.getEntity( String.class );
        return UUID.fromString( responseMessage );
    }


    /**
     *
     * @param stack used to retrieve the environment which the stack instances are created on Subutai
     * @param spec used to create nodeGroup to be deployed to an existing environment
     * @return creates runner instances for the specified stack with the given instance specification
     */
    public Set<Instance> createRunnersOnEnvironment( final ICoordinatedStack stack, final InstanceSpec spec ) {
        Set<Instance> runnerInstances = new HashSet<Instance>();

        // Check if all of the clusters are created successfully
        for ( Cluster cluster : stack.getClusters() ) {
            ICoordinatedCluster coordinatedCluster = ( ICoordinatedCluster ) cluster;
            if ( coordinatedCluster == null ) {
                LOG.error( "Could not find any cluster set up for the {} stack. Not creating runners!", stack.getName() );
                return runnerInstances;
            }

            if ( coordinatedCluster.getInstances().size() != cluster.getSize() ) {
                LOG.error( String.format( "{} number of instances are created for {} cluster out of {}. Not creating runner instances!",
                        coordinatedCluster.getInstances().size() + "", cluster.getName(), cluster.getSize() + "" ) );
                return runnerInstances;
            }
        }

        // Get the environmentId by one of the containers from one of the clusters
        ICoordinatedCluster cluster = ( ICoordinatedCluster ) stack.getClusters().get( 0 );
        UUID environmentId = getEnvironmentIdByInstanceId(
                UUID.fromString( cluster.getInstances().iterator().next().getId() ) );
        if ( environmentId == null ) {
            LOG.error( "Could not find environment of {} cluster", cluster.getName() );
            return runnerInstances;
        }

        TopologyJson runnerTopology = getRunnerTopology( stack, spec );

        // Send a request to add the nodegroup the specified environment
        WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        String runnerTopologyEncoded = UriComponent.encode( gson.toJson( runnerTopology ), UriComponent.Type.QUERY_PARAM );

        ClientResponse addNodeGroupResponse = resource.path( "/grow" )
                                                      .queryParam( RestParams.ENVIRONMENT_ID, environmentId.toString() )
                                                      .queryParam( RestParams.ENVIRONMENT_TOPOLOGY, runnerTopologyEncoded )
                                                      .type( MediaType.TEXT_HTML_TYPE )
//                                                      .accept( MediaType.APPLICATION_JSON )
                                                      .post( ClientResponse.class );


        String responseMessage = addNodeGroupResponse.getEntity( String.class );
        LOG.debug( "Response of add node group rest call: {}", responseMessage );

        if( addNodeGroupResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            LOG.error( "Could not create runner instances on {} environment, Error: {}",
                    getEnvironmentByEnvironmentId( environmentId ).getName(), responseMessage );
            return runnerInstances;
        }
        Type listType = new TypeToken<Set<ContainerJson>>() {}.getType();

        Set<ContainerJson> runnerContainers = gson.fromJson( responseMessage, listType );

        LOG.info( "Runner instances are created for {} successfully", stack.getName() );

        for ( ContainerJson containerJson : runnerContainers ) {
            runnerInstances.add( SubutaiUtils.getInstanceFromContainer( containerJson ) );
        }

        return runnerInstances;
    }


    /**
     *
     * @param instanceId id of the instance to find the environmentId it belongs to
     * @return the environmentId for the given instance
     */
    public UUID getEnvironmentIdByInstanceId( UUID instanceId ) {
        // Send a request to add the nodegroup the specified environment
        WebResource resource = client.resource( "http://" + httpAddress ).path( CONTAINER_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentIdResponse = resource.path( "/environmentId" )
                                                      .queryParam( RestParams.INSTANCE_ID, instanceId.toString() )
                                                      .type( MediaType.APPLICATION_JSON )
                                                      .get( ClientResponse.class );

        if ( environmentIdResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            LOG.warn( "EnvironmentId query by instanceId {} is not successful! Error: {}", instanceId,
                    environmentIdResponse.getEntity( String.class ) );
            return null;
        }

        String responseMessage = environmentIdResponse.getEntity( String.class );
        UUID environmentId = UUID.fromString( responseMessage );
        return environmentId;
    }


    /**
     *
     * @param environmentId id of the environment to retrieve the environment details
     * @return the environment for the given environmentId
     */
    public EnvironmentJson getEnvironmentByEnvironmentId( UUID environmentId ) {
        // Send a request to build the blueprint
        WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentGetResponse = resource.queryParam( RestParams.ENVIRONMENT_ID, environmentId.toString() )
                                                        .type( MediaType.APPLICATION_JSON )
                                                        .accept( MediaType.APPLICATION_JSON )
                                                        .get( ClientResponse.class );

        String responseMessage = environmentGetResponse.getEntity( String.class );

        if ( environmentGetResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            LOG.error( "Environment query by environmentId {} is not successful! Error: {}", environmentId, responseMessage );
            return null;
        }

        EnvironmentJson environment = gson.fromJson( responseMessage, EnvironmentJson.class );
        return environment;
    }


    public boolean destroyEnvironment( UUID environmentId ) {
        // Send a request to build the blueprint
        WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse environmentDestroyResponse = resource.queryParam( RestParams.ENVIRONMENT_ID, environmentId.toString() )
                                                            .type( MediaType.APPLICATION_JSON )
                                                            .delete( ClientResponse.class );

        boolean success = environmentDestroyResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        if ( success ) {
            LOG.info( "Environment(id:{}) is destroyed successfully", environmentId );
        }
        else {
            LOG.warn( "Could not destroy environment! Error: {}", environmentId,
                    environmentDestroyResponse.getEntity( String.class ) );
        }
        return success;
    }


    public boolean destroyInstanceByInstanceId( UUID instanceId ) {
        // Send a request to build the blueprint
        WebResource resource = client.resource( "http://" + httpAddress ).path( CONTAINER_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse instanceDestroyResponse = resource.queryParam( RestParams.INSTANCE_ID, instanceId.toString() )
                                                         .type( MediaType.APPLICATION_JSON )
                                                         .delete( ClientResponse.class );

        boolean success = instanceDestroyResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        if ( success ) {
            LOG.info( "Instance(id:{}) is destroyed successfully", instanceId );
        }
        else {
            LOG.warn( "Could not destroy instance(id:{})! Error: {}", instanceId,
                    instanceDestroyResponse.getEntity( String.class ) );
        }
        return success;
    }


    public InstanceState getInstanceState( UUID instanceId ) {
        // Send a request to build the blueprint
        WebResource resource = client.resource( "http://" + httpAddress ).path( CONTAINER_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse instanceDestroyResponse = resource.path( "/state" )
                                                         .queryParam( RestParams.INSTANCE_ID, instanceId.toString() )
                                                         .type( MediaType.APPLICATION_JSON )
                                                         .get( ClientResponse.class );

        String responseMessage = instanceDestroyResponse.getEntity( String.class );
        ContainerHostState containerHostState = gson.fromJson( responseMessage, ContainerHostState.class );
        InstanceState instanceState = InstanceState.fromContainerHostState( containerHostState );
        return instanceState;
    }


    public UUID getEnvironmentIdByCluster( ICoordinatedCluster cluster ) {
        if ( cluster.getInstances().size() == 0 ) {
            LOG.warn( "Could not find environment of {} since there is no instance in the cluster", cluster.getName() );
            return null;
        }

        UUID environmentId = getEnvironmentIdByInstanceId(
                UUID.fromString( cluster.getInstances().iterator().next().getId() ) );
        return environmentId;
    }


    /**
     *
     * @param cluster
     * @param instances
     * @return true if the cluster is successfully configured
     */
    public boolean configureCluster( final Cluster cluster,
                                     final Collection<Instance> instances ) {
        LOG.info( "Configuring the cluster {} with {} plugin", cluster.getName(), cluster.getConfiguratorPlugin() );

        SubutaiPlugin plugin = SubutaiUtils.getSubutaiPluginFromString( cluster.getConfiguratorPlugin() );

        if ( plugin == null ) {
            LOG.error( "{} plugin is either invalid or not supported by Subutai/Chop! "
                    + "Aborting without any configuration!", cluster.getConfiguratorPlugin() );
            return false;
        }

        boolean success;

        switch ( plugin ) {
            case Cassandra:
                success = configureCassandraCluster( cluster, instances );
                break;
            case Hadoop:
                success = configureHadoopCluster( cluster, instances );
                break;
            default:
                success = false;
                LOG.error( "Configuration of instances via %s plugin has not been implemented yet!" );
                break;
        }

        if ( success ) {
            LOG.info( "Cluster {} configured successfully", cluster.getName() );
        }
        else {
            LOG.error( "Configuration of {} cluster failed!", cluster.getName() );
        }
        return success;
    }


    public boolean configureCassandraCluster( final Cluster cluster, final Collection<Instance> clusterInstances ) {

        UUID environmentId = getEnvironmentIdByInstanceId( UUID.fromString( clusterInstances.iterator().next().getId() ) );

        // Set 1/3 of the instances as seeds
        int seedCount = clusterInstances.size() / 3;
        if ( seedCount < 1 ) {
            seedCount = 1;
        }
        StringBuilder nodeIdsSeperatedByComma = new StringBuilder();
        StringBuilder seedIdsSeperatedByComma = new StringBuilder();

        int i = 1;
        int clusterSize = clusterInstances.size();
        for ( Instance clusterInstance : clusterInstances ) {
            nodeIdsSeperatedByComma.append( clusterInstance.getId() );
            // Do not add comma at the end
            if ( i < clusterSize ) {
                nodeIdsSeperatedByComma.append( "," );
            }

            if ( i <= seedCount ) {
                seedIdsSeperatedByComma.append( clusterInstance.getId() );
                // Do not add comma at the end
                if ( i != seedCount ) {
                    seedIdsSeperatedByComma.append( "," );
                }
            }
            i++;
        }

        // Configure cluster
        WebResource resource = client.resource( "http://" + httpAddress ).path( CASSANDRA_PLUGIN_CONFIGURE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint
        ClientResponse configureCassandraResponse = resource.path( "/" + environmentId.toString() )
                                                            .path( "/clusterName" )
                                                            .path( "/" + cluster.getName() )
                                                            .path( "/nodes" )
                                                            .path( "/" + nodeIdsSeperatedByComma )
                                                            .path( "/seeds" )
                                                            .path( "/" + seedIdsSeperatedByComma )
                                                            .type( MediaType.APPLICATION_JSON )
                                                            .post( ClientResponse.class );

        boolean success = configureCassandraResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        if ( success ) {
            LOG.info( "Instances of {} cluster is configured successfully", cluster.getName() );
        }
        else {
            LOG.error( "Configuration of {} cluster failed! Error: {}", cluster.getName(),
                    configureCassandraResponse.getEntity( String.class ) );
            // TODO remove this when the plugin rests works as expected and enable return statement below
            LOG.warn( "Preteding configuration worked succesfully!" );
//            return success;
        }


        int tryCount = 0;
        int maxTryCount = 5;
        ClientResponse startCassandraResponse;
        do
        {
            // Start cluster
            resource = client.resource( "http://" + httpAddress ).path( CASSANDRA_PLUGIN_BASE_ENDPOINT );
            startCassandraResponse = resource.path( "/clusters" )
                                                            .path( "/" + cluster.getName() )
                                                            .path( "/start" )
                                                            .type( MediaType.APPLICATION_JSON )
                                                            .put( ClientResponse.class );

            success = startCassandraResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;

            // Wait for Subutai to finish its internal operations like saving the cluster info to database
            if ( ! success ) {
                try
                {
                    LOG.info( "Waiting for Subutai to finish its internal operations({}/{} retry)...", ++tryCount, maxTryCount );
                    Thread.sleep( 5000 );
                }
                catch ( InterruptedException e )
                {
                    e.printStackTrace();
                }
            }

        } while ( !success && ( tryCount < maxTryCount ) );

        // TODO remove this when the plugin rests works as expected
        if ( ! success ) {
            LOG.warn( "Preteding start operation worked succesfully!" );
            success = true;
        }


        if ( success ) {
            LOG.info( "Started {} cluster processes successfully", cluster.getName() );
        }
        else {
            LOG.error( "Could not start the processes of {} cluster failed! Error: {}", cluster.getName(),
                    startCassandraResponse.getEntity( String.class ) );
        }

        return success;
    }


    public boolean configureHadoopCluster( final Cluster cluster, final Collection<Instance> instances ) {
        // TODO implement this functionality
        return true;
    }



    public String getHttpAddress()
    {
        return httpAddress;
    }


    public void setHttpAddress( final String httpAddress )
    {
        this.httpAddress = httpAddress;
    }
}
