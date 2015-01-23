package org.apache.usergrid.chop.api.store.subutai;


import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.safehaus.subutai.common.host.ContainerHostState;
import org.safehaus.subutai.core.environment.api.helper.EnvironmentStatusEnum;
import org.safehaus.subutai.core.environment.rest.ContainerJson;
import org.safehaus.subutai.core.environment.rest.EnvironmentJson;

import org.apache.usergrid.chop.api.Commit;
import org.apache.usergrid.chop.api.Module;
import org.apache.usergrid.chop.api.RestParams;
import org.apache.usergrid.chop.stack.Cluster;
import org.apache.usergrid.chop.stack.CoordinatedStack;
import org.apache.usergrid.chop.stack.Stack;
import org.apache.usergrid.chop.stack.User;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.gson.Gson;

import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@FixMethodOrder( MethodSorters.NAME_ASCENDING)
public class TestSubutaiClient
{
    public static SubutaiClient subutaiClient;

    private static CoordinatedStack stack;
    private static EnvironmentJson environmentJson;
    private static EnvironmentJson mockEnvironmentJson;
    private static ContainerJson mockContainerJson;

    private static Commit commit = mock( Commit.class );
    private static Module module = mock( Module.class );

    private static final int RUNNER_COUNT = 2;
    private static final int TEST_PORT = 8089;

    @ClassRule
    public static WireMockRule wireMockRule = new WireMockRule( TEST_PORT );


    @BeforeClass
    public static void init() throws IOException
    {
//                subutaiClient = new SubutaiClient( "172.16.11.188:8181" );
        subutaiClient = new SubutaiClient( "127.0.0.1:" + TEST_PORT );

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

        Set<ContainerJson> containers = new HashSet<ContainerJson>();
        UUID environmentId = stack.getId();
        Cluster cluster = stack.getClusters().get( 0 );
        for ( int i = 0; i < cluster.getSize(); i++ ) {
            mockContainerJson = new ContainerJson( UUID.randomUUID(), environmentId, cluster.getName() + "-" + ( i+1 ),
                    ContainerHostState.RUNNING, "172.16.1." + ( i+1 ), cluster.getInstanceSpec().getImageId() );
            containers.add( mockContainerJson );
        }

        mockEnvironmentJson = new EnvironmentJson( environmentId, stack.getName(),
                EnvironmentStatusEnum.HEALTHY, "", containers );


        //        Stub rest endpoints
        //        Build environment by blueprint
        stubFor( post( urlPathEqualTo( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT ) )
                        .withQueryParam( RestParams.ENVIRONMENT_BLUEPRINT, notMatching( "" ) )
                        .willReturn( aResponse()
                                .withStatus( 200 )
                                .withBody( new Gson().toJson( mockEnvironmentJson ) )
                                   )
               );
        //        Get environment by environmentId
        stubFor( get( urlPathEqualTo( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT ) )
                        .withQueryParam( RestParams.ENVIRONMENT_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                        .withBody( new Gson().toJson( mockEnvironmentJson ) )
                                   )
               );

        //        Get environmentId by instanceId
        stubFor( get( urlPathEqualTo( SubutaiClient.CONTAINER_BASE_ENDPOINT + "/environmentId" ) )
                        .withQueryParam( RestParams.INSTANCE_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                        .withBody( mockEnvironmentJson.getId().toString() )
                                   )
               );

        //        Destroy environment by environmentId
        stubFor( delete( urlPathEqualTo( SubutaiClient.ENVIRONMENT_BASE_ENDPOINT ) )
                        .withQueryParam( RestParams.ENVIRONMENT_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                   )
               );


        //        Destroy instance by instanceId
        stubFor( delete( urlPathEqualTo( SubutaiClient.CONTAINER_BASE_ENDPOINT ) )
                        .withQueryParam( RestParams.INSTANCE_ID, notMatching( "" ) )
                        .willReturn( aResponse()
                                        .withStatus( 200 )
                                   )
               );

    }


    @Test
    public void test1_createStackEnvironment() {
        environmentJson = subutaiClient
                .createStackEnvironment( stack );
        assertNotNull( environmentJson );
    }


    @Test
    public void test2_getEnvironmentByEnvironmentId() {
        EnvironmentJson environmentByEnvironmentId = subutaiClient
                .getEnvironmentByEnvironmentId( environmentJson.getId() );
        assertEquals( environmentByEnvironmentId.getId(), environmentJson.getId() );
    }



    @Test
    public void test3_getEnvironmentIdByInstanceId() {
        UUID environmentIdByInstanceId = subutaiClient
                .getEnvironmentIdByInstanceId( environmentJson.getContainers().iterator().next().getId() );
        assertEquals( environmentIdByInstanceId, environmentJson.getId() );
    }


    @Test
    public void test4_destroyInstance() {
        ContainerJson instance = environmentJson.getContainers().iterator().next();
        boolean isDestroySuccesful = subutaiClient.destroyInstanceByInstanceId( instance.getId() );
        assertTrue( isDestroySuccesful );
    }


    @Test
    public void test5_destroyEnvironment() {
        boolean isDestroySuccesful = subutaiClient.destroyEnvironment( environmentJson.getId() );
        assertTrue( isDestroySuccesful );
    }
}
