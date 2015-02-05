package org.apache.usergrid.chop.example.cassandra;


import org.apache.usergrid.chop.api.IterationChop;
import org.apache.usergrid.chop.example.subutai.cassandra.CassandraClient;
import org.apache.usergrid.chop.example.subutai.cassandra.RandomDataGenerator;
import org.apache.usergrid.chop.example.subutai.cassandra.SimpleCassandraClient;
import org.apache.usergrid.chop.stack.ChopCluster;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.Instance;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

import static junit.framework.TestCase.assertNotNull;


@FixMethodOrder( MethodSorters.NAME_ASCENDING)
@IterationChop( iterations = 10, threads = 1 )
public class CassandraClusterTest {
    private static final Logger LOG = LoggerFactory.getLogger( SimpleCassandraClient.class );

    // Cluster name should be same the same with the cluster defined inside stack.json file
    @ChopCluster( name = "TestCassandraCluster" )
    public static ICoordinatedCluster testCluster;


    public static CassandraClient client = new SimpleCassandraClient();
    public static RandomDataGenerator dataGenerator = new RandomDataGenerator( client );


    @BeforeClass
    public static void initConnection() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping initConnection()" );
            return;
        }
        String connectionIP = getIPList( testCluster ).get( 0 );
        LOG.info( "Creating connection with {}", connectionIP );
        client.connect( connectionIP );
    }


    @Test
    public void test001_connectionTest() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping connectionTest()" );
            return;
        }
        assertNotNull( client.getSession() );
        Metadata metadata = client.getCluster().getMetadata();
        assertNotNull( metadata );
        for ( KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces() ) {
            LOG.info( "Found keyspace: {}", keyspaceMetadata.getName() );
        }
    }


    @Test
    public void test002_dataLoadTest() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping checkSchema()" );
            return;
        }
        dataGenerator.createTestKeyspaceIfNotExists();
        dataGenerator.createTestTablesIfNotExists();
        dataGenerator.generateRandomData( 1000 );
    }


    @AfterClass
    public static void closeConnection() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping closeConnection()" );
            return;
        }
        String connectionIP = getIPList( testCluster ).get( 0 );
        LOG.info( "Closing connection with {}", connectionIP );
        client.close();
    }


    private static ArrayList<String> getIPList( ICoordinatedCluster cluster ){
        ArrayList<String> ipList = new ArrayList<String>();
        for ( Instance temp : cluster.getInstances() ) {
            ipList.add( temp.getPrivateIpAddress() );
        }
        return ipList;
    }
}
