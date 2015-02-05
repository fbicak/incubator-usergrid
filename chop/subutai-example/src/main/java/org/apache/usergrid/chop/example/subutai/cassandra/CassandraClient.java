package org.apache.usergrid.chop.example.subutai.cassandra;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public interface CassandraClient {
    void connect( String node );
    void close();
    boolean isConnected();
    Cluster getCluster();
    void setCluster( Cluster cluster );
    Session getSession();
    void setSession( Session session );
}
