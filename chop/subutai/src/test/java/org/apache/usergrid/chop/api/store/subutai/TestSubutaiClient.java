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


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;
import org.safehaus.subutai.common.environment.EnvironmentStatus;
import org.safehaus.subutai.common.host.ContainerHostState;
import org.safehaus.subutai.core.env.rest.ContainerJson;
import org.safehaus.subutai.core.env.rest.EnvironmentJson;

import org.apache.usergrid.chop.api.Commit;
import org.apache.usergrid.chop.api.Module;
import org.apache.usergrid.chop.api.RestParams;
import org.apache.usergrid.chop.spi.LaunchResult;
import org.apache.usergrid.chop.stack.Cluster;
import org.apache.usergrid.chop.stack.CoordinatedStack;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.Instance;
import org.apache.usergrid.chop.stack.InstanceSpec;
import org.apache.usergrid.chop.stack.InstanceState;
import org.apache.usergrid.chop.stack.Stack;
import org.apache.usergrid.chop.stack.User;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@FixMethodOrder( MethodSorters.NAME_ASCENDING)
public class TestSubutaiClient
{
    private static SubutaiFig subutaiFig;
    private static SubutaiInstanceManager subutaiInstanceManager;
    private static SubutaiClient subutaiClient;
    private static File publicKeyFile;

    private static CoordinatedStack stack;
    private static EnvironmentJson environmentJson;
    private static EnvironmentJson mockClusterEnvironmentJson;
    private static ContainerJson mockClusterContainerJson;
    private static ContainerJson mockRunnerContainerJson;

    private static Commit commit = mock( Commit.class );
    private static Module module = mock( Module.class );


    private static final int RUNNER_COUNT = 2;
    private static final int TEST_PORT = 8089;

    @ClassRule
    public static WireMockRule wireMockRule = new WireMockRule( TEST_PORT );

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void init() throws IOException
    {
        Injector injector = Guice.createInjector( new SubutaiModule() );
        subutaiFig = injector.getInstance( SubutaiFig.class );
        subutaiFig.bypass( SubutaiFig.SUBUTAI_PEER_SITE, "127.0.0.1:" + TEST_PORT );
        subutaiClient = new SubutaiClient( "127.0.0.1:" + TEST_PORT );
        subutaiInstanceManager = new SubutaiInstanceManager( subutaiFig );

        ObjectMapper mapper = new ObjectMapper();
        InputStream is = TestSubutaiClient.class.getClassLoader().getResourceAsStream( "test-stack.json" );
        Stack basicStack = mapper.readValue( is, Stack.class );

        /** Commit mock object get method values */
        when( commit.getCreateTime() ).thenReturn( new Date() );
        when( commit.getMd5() ).thenReturn( "742e2a76a6ba161f9efb87ce58a9187e" );
        when( commit.getModuleId() ).thenReturn( "2000562494" );
        when( commit.getRunnerPath() ).thenReturn( "/some/dummy/path" );
        when( commit.getId() ).thenReturn( "cc471b502aca2791c3a068f93d15b79ff6b7b827" );

        /** Module mock object get method values */
        when( module.getGroupId() ).thenReturn( "org.apache.usergrid.chop" );
        when( module.getArtifactId() ).thenReturn( "chop-maven-plugin" );
        when( module.getVersion() ).thenReturn( "1.0-SNAPSHOT" );
        when( module.getVcsRepoUrl() ).thenReturn( "https://stash.safehaus.org/scm/chop/main.git" );
        when( module.getTestPackageBase() ).thenReturn( "org.apache.usergrid.chop" );
        when( module.getId() ).thenReturn( "2000562494" );
        stack = new CoordinatedStack( basicStack, new User( "user", "pass" ), commit, module, RUNNER_COUNT );

        Set<ContainerJson> clusterContainers = new HashSet<ContainerJson>();
        UUID environmentId = stack.getId();
        Cluster cluster = stack.getClusters().get( 0 );
        for ( int i = 0; i < cluster.getSize(); i++ ) {
            mockClusterContainerJson = new ContainerJson( UUID.randomUUID(), environmentId,
                    cluster.getName() + "-" + ( i+1 ),
                    ContainerHostState.RUNNING, "172.16.1." + ( i+1 ), cluster.getInstanceSpec().getImageId() );
            clusterContainers.add( mockClusterContainerJson );
        }

        Set<ContainerJson> runnerContainers = new HashSet<ContainerJson>();
        for ( int i = 0; i < stack.getRunnerCount(); i++ ) {
            mockRunnerContainerJson = new ContainerJson( UUID.randomUUID(), environmentId,
                    stack.getName() + "-runner-" + ( i+1 ),
                    ContainerHostState.RUNNING, "172.16.2." + ( i+1 ), "runnerTemplate" );
            runnerContainers.add( mockRunnerContainerJson );
        }

        mockClusterEnvironmentJson = new EnvironmentJson( environmentId, stack.getName(),
                EnvironmentStatus.HEALTHY, clusterContainers );

        /**  */
        String publicKeyFileName = SubutaiUtils.getPublicKeyFileName( cluster.getInstanceSpec().getKeyName() );
        File file = folder.newFile( publicKeyFileName );
        publicKeyFile = file;
        // Stub rest endpoints
        // Build environment by blueprint
        stubFor( post( urlPathEqualTo( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT ) ).willReturn(
                        aResponse().withStatus( 200 ).withBody( new Gson().toJson( mockClusterEnvironmentJson ) ) ) );

        // Add new clusterContainers to an existing environment by topology
        stubFor( post( urlPathEqualTo( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT + "/grow" ) )
                        .willReturn( aResponse().withStatus( 200 ).withBody( new Gson().toJson( runnerContainers ) )
                                   ) );

        // Configure cluster
        stubFor( post( urlMatching( SubutaiClient.CASSANDRA_PLUGIN_CONFIGURE_ENDPOINT
                        + "/.+"
                        +  "/clusterName"
                        + "/.+"
                        +  "/nodes"
                        +  "/.+"
                        +  "/seeds"
                        +  "/.+"
                                  )
                     )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                   )
               );

        // Start cluster
        stubFor( put( urlMatching( SubutaiClient.CASSANDRA_PLUGIN_BASE_ENDPOINT + "/clusters" + "/.+" + "/start"

                                 ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                   )
               );

        // Get environment by environmentId
        stubFor( get( urlMatching( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT + "/.+" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                        .withBody( new Gson().toJson( mockClusterEnvironmentJson ) )
                                   )
               );

        // Get environmentId by instanceId
        stubFor( get( urlPathEqualTo( SubutaiClient.CONTAINER_BASE_ENDPOINT + "/environmentId" ) )
                        .withQueryParam( RestParams.INSTANCE_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                        .withBody( mockClusterEnvironmentJson.getId().toString() )
                                   )
               );

        // Destroy environment by environmentId
        stubFor( delete( urlPathEqualTo( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT ) )
                        .withQueryParam( RestParams.ENVIRONMENT_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                   )
               );


        // Destroy instance by instanceId
        stubFor( delete( urlPathEqualTo( SubutaiClient.CONTAINER_BASE_ENDPOINT ) )
                        .withQueryParam( RestParams.INSTANCE_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                   )
               );


        // Get instance state by instanceId
        stubFor( get( urlPathEqualTo( SubutaiClient.CONTAINER_BASE_ENDPOINT + "/state" ) )
                        .withQueryParam( RestParams.INSTANCE_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                        .withBody( new Gson().toJson( ContainerHostState.RUNNING ) )
                                   )
               );

        // Get peer id of Subutai
        stubFor( get( urlPathEqualTo( SubutaiClient.PEER_BASE_ENDPOINT + "/id" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                        .withBody( UUID.randomUUID().toString() )
                                   )
               );
    }


    @Test
    public void test001_createStackEnvironment() {
        environmentJson = subutaiClient.createStackEnvironment( stack, publicKeyFile );
        assertNotNull( environmentJson );
        for ( ContainerJson containerJson : environmentJson.getContainers() ) {
            stack.getClusters().get( 0 ).add( SubutaiUtils.getInstanceFromContainer( containerJson ) );
        }
    }


    @Test
    public void test002_getEnvironmentByEnvironmentId() {
        EnvironmentJson environmentByEnvironmentId = subutaiClient
                .getEnvironmentByEnvironmentId( environmentJson.getId() );
        assertEquals( environmentByEnvironmentId.getId(), environmentJson.getId() );
    }



    @Test
    public void test003_getEnvironmentIdByInstanceId() {
        UUID environmentIdByInstanceId = subutaiClient
                .getEnvironmentIdByInstanceId( environmentJson.getContainers().iterator().next().getId() );
        assertEquals( environmentIdByInstanceId, environmentJson.getId() );
    }


    @Test
    public void test004_configureCluster() {
        Cluster cluster = stack.getClusters().get( 0 );
        boolean success = subutaiClient.configureCluster( cluster, stack.getClusters().get( 0 ).getInstances() );
        assertTrue( success );
    }


    @Test
    public void test005_checkInstanceState() {
        UUID instanceId = UUID.fromString( stack.getClusters().get( 0 ).getInstances().iterator().next().getId() );
        InstanceState instanceState = subutaiClient.getInstanceState( instanceId );
        assertEquals( InstanceState.fromContainerHostState( ContainerHostState.RUNNING ), instanceState );
    }


    @Test
    public void test006_createRunnersOnStackEnvironment() {
        InstanceSpec spec = stack.getClusters().get( 0 ).getInstanceSpec();
        Set<Instance> runnerInstances = subutaiClient.createRunnersOnEnvironment( stack, spec );
        stack.getRunnerInstances().addAll( runnerInstances );
        assertEquals( stack.getRunnerCount(), runnerInstances.size() );
        assertEquals( stack.getRunnerInstances().size(), runnerInstances.size() );
    }


    @Test
    public void test007_destroyInstance() {
        int environmentSizeBeforeDestroy = environmentJson.getContainers().size();
        ContainerJson instance = environmentJson.getContainers().iterator().next();
        boolean isDestroySuccesful = subutaiClient.destroyInstanceByInstanceId( instance.getId() );
        assertTrue( isDestroySuccesful );
        environmentJson.getContainers().remove( instance );
        int environmentSizeAfterDestroy = environmentJson.getContainers().size();
        assertEquals( environmentSizeBeforeDestroy, environmentSizeAfterDestroy + 1 );
    }


    @Test
    public void test008_destroyEnvironment() {
        boolean isDestroySuccesful = subutaiClient.destroyEnvironment( environmentJson.getId() );
        assertTrue( isDestroySuccesful );
        environmentJson.getContainers().clear();
        assertEquals( environmentJson.getContainers().size(), 0 );
    }


    @Test
    public void test009_launchCluster() {
        ICoordinatedCluster cluster = stack.getClusters().get( 0 );
        LaunchResult launchResult = subutaiInstanceManager.launchCluster( stack, cluster, 300, publicKeyFile.getAbsolutePath() );
        assertEquals( launchResult.getCount(), cluster.getSize() );
    }


    @Test
    public void test010_launchRunners() {
        ICoordinatedCluster cluster = stack.getClusters().get( 0 );
        LaunchResult launchResult = subutaiInstanceManager.launchRunners( stack, cluster.getInstanceSpec(), 300 );
        assertEquals( launchResult.getCount(), stack.getRunnerCount() );
    }


    @Test
    public void test011_terminateInstances() {
        ICoordinatedCluster cluster = stack.getClusters().get( 0 );
        Collection<String> terminateIds = new ArrayList<String>();
        for ( Instance clusterInstance : cluster.getInstances() ) {
            terminateIds.add( clusterInstance.getId() );
        }
        subutaiInstanceManager.terminateInstances( terminateIds );
    }


    @Test
    public void test012_terminateAllInstances() {
        Collection<String> terminateIds = new ArrayList<String>();
        for ( ICoordinatedCluster cluster : stack.getClusters() ) {
            for ( Instance clusterInstance : cluster.getInstances() ) {
                terminateIds.add( clusterInstance.getId() );
            }
        }

        for ( Instance runnerInstance : stack.getRunnerInstances() ) {
            terminateIds.add( runnerInstance.getId() );
        }
        subutaiInstanceManager.terminateInstances( terminateIds );
    }


    @Test
    public void test013_createStackEnvironmentWithoutPublicKey() {
        environmentJson = subutaiClient.createStackEnvironment( stack, null );
        assertNull( environmentJson );
    }


    @Test
    public void test014_launchClusterWithoutPublicKey() {
        ICoordinatedCluster cluster = stack.getClusters().get( 0 );
        LaunchResult launchResult = subutaiInstanceManager.launchCluster( stack, cluster, 300,
                publicKeyFile.getAbsolutePath() + "some-dummy-file-name" );
        assertEquals( launchResult.getCount(), 0 );
    }
}
