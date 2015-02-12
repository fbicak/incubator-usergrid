package org.apache.usergrid.chop.example.subutai.cassandra;


import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;


public class SimpleCassandraClient implements CassandraClient {

    private static final Logger LOG = LoggerFactory.getLogger( SimpleCassandraClient.class );
    private Cluster cluster;
    private Session session;


    @Override
    public synchronized void connect( List<String> nodeList ) {
        Preconditions.checkNotNull( nodeList );
        Preconditions.checkState( nodeList.size() != 0 );

        Cluster.Builder builder = Cluster.builder();
        for ( String node : nodeList ) {
            builder = builder.addContactPoint( node );
        }

        setCluster( builder.build() );
        Metadata metadata = getCluster().getMetadata();
        LOG.info( "Connected to cluster {}.", metadata.getClusterName() );

        for ( Host host : metadata.getAllHosts() ) {
            LOG.info( "DataCenter: {}, Host: {}, Rack: {}.", new Object[]{ host.getDatacenter(), host.getAddress(), host.getRack() } );
        }
        setSession( cluster.connect() );
    }


    @Override
    public synchronized void close() {
        try {
            LOG.info( "Closing connection with the cluster {}", getCluster().getClusterName() );
            getCluster().close();
        } catch ( RejectedExecutionException rejectedExecutionException ) {
            LOG.warn( "Could not close the cluster session." );
        }
    }


    @Override
    public boolean isConnected() {
        if ( session != null )
            return true;
        return false;
    }

    private Cluster getCluster() {
        return cluster;
    }


    private synchronized void setCluster( Cluster cluster ) {
        this.cluster = cluster;
    }


    @Override
    public Session getSession() {
        return session;
    }


    private synchronized void setSession( Session session ) {
        this.session = session;
    }

}