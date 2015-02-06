package org.apache.usergrid.chop.example.subutai.cassandra;


import java.util.List;

import com.datastax.driver.core.Session;


public interface CassandraClient {
    void connect( List<String> node );
    void close();
    Session getSession();
    boolean isConnected();

}
