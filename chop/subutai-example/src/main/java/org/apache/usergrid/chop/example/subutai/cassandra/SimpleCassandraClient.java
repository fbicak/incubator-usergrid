package org.apache.usergrid.chop.example.subutai.cassandra;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public class SimpleCassandraClient implements CassandraClient {

    private static final Logger LOG = LoggerFactory.getLogger( SimpleCassandraClient.class );
    private Cluster cluster;
    private Session session;


    @Override
    public void connect( String node ) {
        setCluster( Cluster.builder().addContactPoint( node ).build() );
        Metadata metadata = getCluster().getMetadata();
        LOG.info( "Connected to cluster {}.", metadata.getClusterName() );

        for ( Host host : metadata.getAllHosts() ) {
            LOG.info( "DataCenter: {}, Host: {}, Rack: {}.", new Object[]{ host.getDatacenter(), host.getAddress(), host.getRack() } );
        }
        session = cluster.connect();
    }


    @Override
    public void close() {
        getCluster().close();
    }


    @Override
    public boolean isConnected() {
        if ( session != null )
            return true;
        return false;
    }

    @Override
    public Cluster getCluster() {
        return cluster;
    }


    @Override
    public void setCluster( Cluster cluster ) {
        this.cluster = cluster;
    }


    @Override
    public Session getSession() {
        return session;
    }


    @Override
    public void setSession( Session session ) {
        this.session = session;
    }

}