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
import com.sun.jersey.api.representation.Form;
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
import org.safehaus.subutai.plugin.hadoop.rest.TrimmedHadoopConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class SubutaiClient
{
    private static final Logger LOG = LoggerFactory.getLogger( SubutaiClient.class );

    private String httpAddress;
    private Client client;
    private String authToken;

    public static final String REST_BASE_ENDPOINT = "/cxf";
    public static final String ENVIRONMENT_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/environments";
    public static final String PEER_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/peer";
    public static final String CONTAINER_BASE_ENDPOINT = ENVIRONMENT_BASE_ENDPOINT + "/container";
    public static final String CASSANDRA_PLUGIN_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/cassandra";
    public static final String HADOOP_PLUGIN_BASE_ENDPOINT = REST_BASE_ENDPOINT + "/hadoop";
    public static final String CASSANDRA_PLUGIN_CONFIGURE_ENDPOINT = CASSANDRA_PLUGIN_BASE_ENDPOINT + "/configure_environment";
    public static final String HADOOP_PLUGIN_CONFIGURE_ENDPOINT = HADOOP_PLUGIN_BASE_ENDPOINT + "/configure_environment";
    private Gson gson;


    public SubutaiClient( SubutaiFig subutaiFig ) {
        this.httpAddress = subutaiFig.getSubutaiPeerSite();
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        client = Client.create( clientConfig );
        authToken = subutaiFig.getSubutaiAuthToken();
        gson = new Gson();
    }


    /**
     *
     * @param stack used to create an environment on Subutai
     * @param cluster
     * @return the environment created for the given stack
     */
    public Set<Instance> createClusterInstances( final ICoordinatedStack stack, final Cluster cluster,
                                                 File publicKeyFile ) {
        Set<Instance> clusterInstances = new HashSet<Instance>();

        String publicKeyFileContent;
        if( publicKeyFile == null  ) {
            LOG.error( "File cannot be null! Aborting.." );
            return null;
        }
        if ( ! publicKeyFile.exists() ) {
            LOG.error( "File {} does not exist! Aborting..", publicKeyFile );
            return null;
        }

        try {
            byte[] encoded = Files.readAllBytes( Paths.get( publicKeyFile.getAbsolutePath() ) );
            publicKeyFileContent = new String( encoded );
        }
        catch ( IOException e ) {
            LOG.error( "Could not read file {}! Error: {}", publicKeyFile.getAbsoluteFile(), e.getMessage() );
            return null;
        }

        // Create the topology from the supplied stack
        TopologyJson topology;
        try {
            topology = getTopologyFromCluster( cluster );
        }
        catch ( SubutaiException e ) {
            LOG.error( e.getMessage() );
            return null;
        }

        // Check if the environment should be created from scratch or should be grown
        ICoordinatedCluster firstCluster = ( ICoordinatedCluster ) stack.getClusters().get( 0 );
        boolean isEnvironmentCreated = ! firstCluster.getInstances().isEmpty();

        if ( ! isEnvironmentCreated ) {
            // Send a request to build the topology
            WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );

            // Returns the uuid of the environment created from the supplied topology
            Form environmentCreateForm = new Form();
            environmentCreateForm.add( RestParams.ENVIRONMENT_NAME, gson.toJson( stack.getName() ) );
            // TODO get free subnets via a rest call when it is implemented on Subutai
            environmentCreateForm.add( RestParams.ENVIRONMENT_SUBNET, "192.168.179.1/24" );
            environmentCreateForm.add( RestParams.ENVIRONMENT_TOPOLOGY, gson.toJson( topology ) );
            environmentCreateForm.add( RestParams.SSH_KEY, publicKeyFileContent );

            ClientResponse environmentBuildResponse = resource.queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                                              .type( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
                                                              .accept( MediaType.APPLICATION_JSON )
                                                              .post( ClientResponse.class, environmentCreateForm );

            if ( environmentBuildResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
                LOG.error( "Instance creation operation for {} cluster is not successful! Error: {}", cluster.getName(),
                        environmentBuildResponse.getEntity( String.class ) );
                return null;
            }
            LOG.info( "Instances for {} is created successfully", cluster.getName() );

            String responseMessage = environmentBuildResponse.getEntity( String.class );
            EnvironmentJson environment = gson.fromJson( responseMessage, EnvironmentJson.class );
            for ( ContainerJson containerJson : environment.getContainers() ) {
                Instance instance = SubutaiUtils.getInstanceFromContainer( containerJson );
                clusterInstances.add( instance );
            }
        }
        else {
            // Get the environmentId by one of the containers from one of the clusters
            UUID environmentId = getEnvironmentIdByInstanceId(
                    UUID.fromString( firstCluster.getInstances().iterator().next().getId() ) );
            if ( environmentId == null ) {
                LOG.error( "Could not find environment of {} cluster", cluster.getName() );
                return null;
            }

            // Send a request to add the nodegroup the specified environment
            WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );
            // Returns the uuid of the environment created from the supplied blueprint

            Form addContainerToExistingEnvironmentForm = new Form();
            addContainerToExistingEnvironmentForm.add( RestParams.ENVIRONMENT_ID, environmentId.toString() );
            addContainerToExistingEnvironmentForm.add( RestParams.ENVIRONMENT_TOPOLOGY, gson.toJson( topology ) );
            addContainerToExistingEnvironmentForm.add( RestParams.SSH_KEY, null );

            ClientResponse addNodeGroupResponse = resource.path( "/grow" )
                                                          .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                                          .type( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
                                                          .accept( MediaType.APPLICATION_JSON )
                                                          .post( ClientResponse.class, addContainerToExistingEnvironmentForm );

            String responseMessage = addNodeGroupResponse.getEntity( String.class );
            LOG.debug( "Response of add node group rest call: {}", responseMessage );

            if( addNodeGroupResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
                LOG.error( "Could not create cluster instances on {} environment, Error: {}",
                        getEnvironmentByEnvironmentId( environmentId ).getName(), responseMessage );
                return clusterInstances;
            }
            Type listType = new TypeToken<Set<ContainerJson>>() {}.getType();

            Set<ContainerJson> clusterContainers = gson.fromJson( responseMessage, listType );

            LOG.info( "Cluster instances are created for {} successfully", cluster.getName() );
            for ( ContainerJson containerJson : clusterContainers ) {
                clusterInstances.add( SubutaiUtils.getInstanceFromContainer( containerJson ) );
            }
        }
        return clusterInstances;
    }


    public TopologyJson getTopologyFromCluster( final Cluster cluster ) throws SubutaiException {
        TopologyJson topology = new TopologyJson();
        Set<NodeGroup> clusterNodeGroups = new HashSet<NodeGroup>( 1 );

        NodeGroup clusterNodeGroup = SubutaiUtils.getClusterNodeGroup( cluster );
        clusterNodeGroups.add( clusterNodeGroup );
        Map<UUID, Set<NodeGroup>> nodeGroupPlacement = new HashMap<UUID, Set<NodeGroup>>();
        UUID localPeerId = getLocalPeerId();
        nodeGroupPlacement.put( localPeerId, clusterNodeGroups );

        topology.setNodeGroupPlacement( nodeGroupPlacement );
        return topology;
    }



    public TopologyJson getRunnerTopology( final ICoordinatedStack stack, InstanceSpec spec ) throws SubutaiException {
        Set<NodeGroup> runnerNodeGroupSet = new HashSet<NodeGroup>();
        runnerNodeGroupSet.add( SubutaiUtils.getRunnerNodeGroup( stack, spec ) );

        TopologyJson topology = new TopologyJson();

        Map<UUID, Set<NodeGroup>> nodeGroupPlacement = new HashMap<UUID, Set<NodeGroup>>();
        UUID localPeerId = getLocalPeerId();
        nodeGroupPlacement.put( localPeerId, runnerNodeGroupSet );

        topology.setNodeGroupPlacement( nodeGroupPlacement );
        return topology;
    }


    public UUID getLocalPeerId() throws SubutaiException {
        // Send a request to build the topology
        WebResource resource = client.resource( "http://" + httpAddress ).path( PEER_BASE_ENDPOINT );

        // Returns the uuid of the peer
        ClientResponse peerIdResponse = resource.path( "id" )
                                                .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                                .get( ClientResponse.class );


        if ( peerIdResponse.getStatus() != Response.Status.OK.getStatusCode() ) {
            String errorMessage = String.format( "Could not retrieve the id of local peer! Error: %s", peerIdResponse.getEntity( String.class ) );
            throw new SubutaiException( errorMessage );
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
                LOG.error( String.format( "%s number of instances are created for %s cluster out of %s. Not creating runner instances!",
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

        TopologyJson runnerTopology;
        try {
            runnerTopology = getRunnerTopology( stack, spec );
        }
        catch ( SubutaiException e ) {
            LOG.error( e.getMessage() );
            return runnerInstances;
        }

        // Send a request to add the nodegroup the specified environment
        WebResource resource = client.resource( "http://" + httpAddress ).path( ENVIRONMENT_BASE_ENDPOINT );
        // Returns the uuid of the environment created from the supplied blueprint

        Form addContainerToExistingEnvironmentForm = new Form();
        addContainerToExistingEnvironmentForm.add( RestParams.ENVIRONMENT_ID, environmentId.toString() );
        addContainerToExistingEnvironmentForm.add( RestParams.ENVIRONMENT_TOPOLOGY, gson.toJson( runnerTopology ) );
        addContainerToExistingEnvironmentForm.add( RestParams.SSH_KEY, null );

        ClientResponse addNodeGroupResponse = resource.path( "/grow" )
                                                      .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                                      .type( MediaType.APPLICATION_FORM_URLENCODED_TYPE )
                                                      .accept( MediaType.APPLICATION_JSON )
                                                      .post( ClientResponse.class, addContainerToExistingEnvironmentForm );

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
                                                      .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
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
        ClientResponse environmentGetResponse = resource.path( environmentId.toString() )
                                                        .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
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
                                                            .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
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
                                                         .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
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
                                                         .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
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
                                                            .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                                            .type( MediaType.APPLICATION_JSON )
                                                            .post( ClientResponse.class );

        boolean success = configureCassandraResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        if ( success ) {
            LOG.info( "Instances of {} cluster is configured successfully", cluster.getName() );
        }
        else {
            LOG.error( "Configuration of {} cluster failed! Error: {}", cluster.getName(),
                    configureCassandraResponse.getEntity( String.class ) );
            return success;
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
                                             .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
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


        if ( success ) {
            LOG.info( "Started {} cluster processes successfully", cluster.getName() );
        }
        else {
            LOG.error( "Could not start the processes of {} cluster failed! Error: {}", cluster.getName(),
                    startCassandraResponse.getEntity( String.class ) );
        }

        return success;
    }


    public boolean configureHadoopCluster( final Cluster cluster, final Collection<Instance> clusterInstances ) {
        List<String> clusterInstanceIds = new ArrayList<String>( clusterInstances.size() );
        for ( Instance clusterInstance : clusterInstances ) {
            clusterInstanceIds.add( clusterInstance.getId() );
        }

        UUID environmentId = getEnvironmentIdByInstanceId( UUID.fromString( clusterInstances.iterator().next().getId() ) );

        TrimmedHadoopConfig hadoopClusterConfig = new TrimmedHadoopConfig();
        int nameNodeIndex = 0;
        int jobTrackerIndex = clusterInstanceIds.size() > 1 ? 1 : 0;
        int secondaryNameNodeIndex = clusterInstanceIds.size() > 1 ? 1 : 0;
        int slaveNodeStartIndex = clusterInstanceIds.size() > 2 ? 2 : 0;

        hadoopClusterConfig.setNameNode( clusterInstanceIds.get( nameNodeIndex ) );
        hadoopClusterConfig.setJobTracker( clusterInstanceIds.get( jobTrackerIndex ) );
        hadoopClusterConfig.setSecNameNode( clusterInstanceIds.get( secondaryNameNodeIndex ) );
        Set<String> slaveNodeIds = new HashSet<String>( clusterInstanceIds.size() - slaveNodeStartIndex );
        for ( int i = slaveNodeStartIndex; i < clusterInstanceIds.size(); i++ ) {
            slaveNodeIds.add( clusterInstanceIds.get( i ) );
        }
        hadoopClusterConfig.setEnvironmentId( environmentId.toString() );
        hadoopClusterConfig.setSlaves( slaveNodeIds );
        hadoopClusterConfig.setClusterName( cluster.getName() );

        // Configure cluster
        WebResource resource = client.resource( "http://" + httpAddress ).path( HADOOP_PLUGIN_CONFIGURE_ENDPOINT );
        ClientResponse configureHadoopResponse = null;
        // Returns the uuid of the environment created from the supplied blueprint
        try {
            String encodedHadoopClusterConfig = UriComponent.encode( gson.toJson( hadoopClusterConfig ), UriComponent.Type.QUERY_PARAM );
            configureHadoopResponse = resource
                    .queryParam( RestParams.SUBUTAI_CONFIG_PARAM_NAME, encodedHadoopClusterConfig )
                    .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                    .type( MediaType.APPLICATION_JSON )
                    .post( ClientResponse.class );
        } catch ( Exception e ) {
            LOG.error( "An error occurred while configuring the hadoop cluster! Error: {}", e );
            return false;
        }


        boolean success = configureHadoopResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;
        if ( success ) {
            LOG.info( "Instances of {} cluster is configured successfully", cluster.getName() );
        }
        else {
            LOG.error( "Configuration of {} cluster failed! Error: {}", cluster.getName(),
                    configureHadoopResponse.getEntity( String.class ) );
            return success;
        }


        ClientResponse startNameNodeResponse;
        // Start cluster
        resource = client.resource( "http://" + httpAddress ).path( HADOOP_PLUGIN_BASE_ENDPOINT );
        try {
            startNameNodeResponse = resource.path( "/clusters" )
                                            .path( "/" + cluster.getName() )
                                            .path( "/start" )
                                            .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                            .put( ClientResponse.class );
        } catch ( Exception e ) {
            LOG.error( "An error occurred while starting the Namenode! Error: {}", e );
            return false;
        }

        success = startNameNodeResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;

        if ( success ) {
            LOG.info( "Started {} cluster Namenode processes successfully", cluster.getName() );
        }
        else {
            LOG.error( "Could not start the NameNode processes of {} cluster failed! Error: {}", cluster.getName(),
                    startNameNodeResponse.getEntity( String.class ) );
            return success;
        }


        ClientResponse startJobTrackerResponse;
        // Start cluster
        resource = client.resource( "http://" + httpAddress ).path( HADOOP_PLUGIN_BASE_ENDPOINT );
        try {
            startJobTrackerResponse = resource.path( "/clusters" )
                                            .path( "/job" )
                                            .path( "/" + cluster.getName() )
                                            .path( "/start" )
                                            .queryParam( RestParams.SUBUTAI_AUTH_TOKEN_NAME, authToken )
                                            .put( ClientResponse.class );
        }  catch ( Exception e ) {
            LOG.error( "An error occurred while starting the JobTracker! Error: {}", e );
            return false;
        }

        success = startJobTrackerResponse.getStatus() == Response.Status.OK.getStatusCode() ? true : false;

        if ( success ) {
            LOG.info( "Started {} cluster JobTracker processes successfully", cluster.getName() );
        }
        else {
            LOG.error( "Could not start the JobTracker processes of {} cluster failed! Error: {}", cluster.getName(),
                    startNameNodeResponse.getEntity( String.class ) );
            return success;
        }
        return success;
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
