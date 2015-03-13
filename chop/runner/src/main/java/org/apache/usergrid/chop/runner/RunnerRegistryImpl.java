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
package org.apache.usergrid.chop.runner;


import java.net.MalformedURLException;
import java.net.URL;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.usergrid.chop.api.CoordinatorFig;
import org.apache.usergrid.chop.api.Project;
import org.apache.usergrid.chop.api.RestParams;
import org.apache.usergrid.chop.api.Runner;
import org.apache.usergrid.chop.spi.RunnerRegistry;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.SetupStackState;

import org.safehaus.jettyjam.utils.CertUtils;
import org.safehaus.guicyfig.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;


/**
 * An implementation of the RunnerRegistry SPI interface to hit coordinator services.
 */
@Singleton
public class RunnerRegistryImpl implements RunnerRegistry {
    private static final Logger LOG = LoggerFactory.getLogger( RunnerRegistryImpl.class );

    private CoordinatorFig coordinatorFig;
    private URL endpoint;

    @Inject
    private Runner me;

    @Inject
    private Project project;


    @Inject
    private void setCoordinatorConfig( CoordinatorFig coordinatorFig ) {
        this.coordinatorFig = coordinatorFig;

        try {
            endpoint = new URL( coordinatorFig.getEndpoint() );
        }
        catch ( MalformedURLException e ) {
            LOG.error( "Failed to parse URL for coordinator", e );
        }

        /**
         * This is because we are using self-signed uniform certificates for now,
         * it should be removed if we switch to a CA signed dynamic certificate scheme!
         * */
        javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(
            new javax.net.ssl.HostnameVerifier() {
                public boolean verify( String hostname, javax.net.ssl.SSLSession sslSession) {
                    return hostname.equals( endpoint.getHost() );
                }
            }
        );
        // Need to get the configuration information for the coordinator
        if ( ! CertUtils.isTrusted( endpoint.getHost() ) ) {
            CertUtils.preparations( endpoint.getHost(), endpoint.getPort() );
        }
        Preconditions.checkState( CertUtils.isTrusted( endpoint.getHost() ), "coordinator must be trusted" );
    }


    private WebResource addQueryParameters( WebResource resource, Project project, Runner runner ) {
        return resource.queryParam( RestParams.RUNNER_HOSTNAME, runner.getHostname() )
                .queryParam( RestParams.RUNNER_PORT, String.valueOf( runner.getServerPort() ) )
                .queryParam( RestParams.RUNNER_IPV4_ADDRESS, runner.getIpv4Address() )
                .queryParam( RestParams.MODULE_GROUPID, project.getGroupId() )
                .queryParam( RestParams.MODULE_ARTIFACTID, project.getArtifactId() )
                .queryParam( RestParams.MODULE_VERSION, project.getVersion() )
                .queryParam( RestParams.COMMIT_ID, project.getVcsVersion() )
                .queryParam( RestParams.VCS_REPO_URL, project.getVcsRepoUrl() )
                .queryParam( RestParams.USERNAME, coordinatorFig.getUsername() )
                .queryParam( RestParams.PASSWORD, coordinatorFig.getPassword() );
    }


    @Override
    public List<Runner> getRunners() {
        // get a list of all runners associated with this project
        WebResource resource = Client.create().resource( coordinatorFig.getEndpoint() );
        resource = addQueryParameters( resource, project, me );
        List<Runner> runners = resource.path( coordinatorFig.getRunnersListPath() )
                                  .type( MediaType.APPLICATION_JSON )
                                  .accept( MediaType.APPLICATION_JSON_TYPE )
                                  .get( new GenericType<List<Runner>>() {} );

        LOG.debug( "Got back runners list = {}", runners );

        return runners;
    }


    @Override
    public List<Runner> getRunners( final Runner runner ) {
        List<Runner> runners = getRunners();

        for ( int ii = 0; ii < runners.size(); ii++ ) {
            if ( runners.get( ii ).getHostname().equals( runner.getHostname() ) ) {
                runners.remove( ii );
            }
        }

        return runners;
    }


    @Override
    public void register( final Runner runner ) {
        WebResource resource = Client.create().resource( coordinatorFig.getEndpoint() );
        resource = addQueryParameters( resource, project, runner );
        Boolean result = resource.path( coordinatorFig.getRunnersRegisterPath() )
                                .type( MediaType.APPLICATION_JSON ).post( Boolean.class, runner );

        LOG.debug( "Got back results from register post = {}", result );
    }


    @Override
    public void unregister( final Runner runner ) {
        if ( RunnerConfig.isTestMode() ) {
            return;
        }

        WebResource resource = Client.create().resource( coordinatorFig.getEndpoint() );
        resource = addQueryParameters( resource, project, runner );
        String result = resource.path( coordinatorFig.getRunnersUnregisterPath() )
                                .type( MediaType.TEXT_PLAIN ).post( String.class );

        LOG.debug( "Got back results from unregister post = {}", result );
    }


    @Override
    public SetupStackState getStackState() {

        /** Status  */
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        Client client = Client.create( clientConfig );
        WebResource resource = client.resource( coordinatorFig.getEndpoint() ).path( "/status" );
        ClientResponse resp = resource.queryParam( RestParams.COMMIT_ID, project.getVcsVersion() )
                                      .queryParam( RestParams.MODULE_ARTIFACTID, project.getArtifactId() )
                                      .queryParam( RestParams.MODULE_GROUPID, project.getGroupId() )
                                      .queryParam( RestParams.MODULE_VERSION, project.getVersion() )
                                      .queryParam( RestParams.USERNAME, coordinatorFig.getUsername() )
                                      .type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
                                      .post( ClientResponse.class );

        if ( resp.getStatus() != Response.Status.OK.getStatusCode() && resp.getStatus() != Response.Status.CREATED
                .getStatusCode() ) {
            LOG.error( "Could not get the status from coordinator, HTTP status: {}", resp.getStatus() );
            LOG.error( "Error Message: {}", resp.getEntity( String.class ) );
            return null;
        }

        SetupStackState stackState = resp.getEntity( SetupStackState.class );
        return stackState;
    }


    @Override
    public List<ICoordinatedCluster> getClusters() {
        if( RunnerConfig.isTestMode() ) {
            return Collections.emptyList();
        }

        // Wait for other runners to register to the coordinator
        // before checking the cluster information
        // since getProperties method in PropertiesResource class
        // returns empty cluster information if the stack state is not SetUp
        waitUntil( SetupStackState.SetUp, 600000 );

        WebResource resource = Client.create().resource( coordinatorFig.getEndpoint() );
        StringBuilder sb = new StringBuilder();
        sb.append( coordinatorFig.getUsername() )
          .append( "/" )
          .append( project.getGroupId() )
          .append( "/" )
          .append( project.getArtifactId() )
          .append( "/" )
          .append( project.getVersion() )
          .append( "/" )
          .append( project.getVcsVersion() );

       return resource.path( coordinatorFig.getPropertiesPath() )
                .path( sb.toString() )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .get( new GenericType<List<ICoordinatedCluster>>() { } );
    }


    /*
    Wait for the stack state to be equal to the expected stack state until the specified timeout
     */
    private void waitUntil( final SetupStackState expectedStackState, final int timeout ) {
        final long SLEEP_LENGTH = 10000;
        Calendar cal = Calendar.getInstance();
        cal.setTime( new Date() );
        final long startTime = cal.getTimeInMillis();
        long timePassed;

        do {
            SetupStackState stackState = getStackState();

            // This if case only happens when webapp and runners are started in test mode
            Env environment = Env.getEnvironment();
            if ( ( environment != null && environment.equals( Env.UNIT ) ) ||
                    ( stackState != null && stackState.equals( SetupStackState.JarNotFound ) ) ) {
                LOG.info( "Setting stack state to the expected state since test mode is captured." );
                stackState = expectedStackState;
            }

            if ( stackState != null && stackState.equals( expectedStackState ) ) {
                LOG.info( "Stack state is \"" + expectedStackState + "\" as expected, continuing...");
                return;
            }
            cal.setTime( new Date() );
            timePassed = cal.getTimeInMillis() - startTime;
            try {
                if ( stackState != null )  {
                    LOG.info( "Current stack state: \"" + stackState + "\", expected state: \"" + expectedStackState
                        +"\". Waiting maximum " + timeout + " milliseconds." );
                }
                else {
                    LOG.warn( "Could not get stack state from coordinator!" );
                }
                Thread.sleep( SLEEP_LENGTH );
            }
            catch ( InterruptedException e ) {
                LOG.warn( "Thread interrupted while sleeping", e );
            }
        } while ( timePassed < timeout );
        LOG.warn( "Waiting stack state to get into \"" + expectedStackState + "\" state has timed out!");
    }

}
