package org.apache.usergrid.chop.example.cassandra;


import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.usergrid.chop.api.IterationChop;
import org.apache.usergrid.chop.example.subutai.cassandra.CassandraClient;
import org.apache.usergrid.chop.example.subutai.cassandra.RandomDataGenerator;
import org.apache.usergrid.chop.example.subutai.cassandra.SimpleCassandraClient;
import org.apache.usergrid.chop.stack.ChopCluster;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.Instance;

import com.datastax.driver.core.Metadata;

import static junit.framework.TestCase.assertNotNull;


@FixMethodOrder( MethodSorters.NAME_ASCENDING)
@IterationChop( iterations = 100, threads = 4 )
public class CassandraClusterTest {
    private static final Logger LOG = LoggerFactory.getLogger( CassandraClusterTest.class );

    // Cluster name should be same the same with the cluster defined inside stack.json file
    @ChopCluster( name = "TestCassandraCluster" )
    public static ICoordinatedCluster testCluster;

    public CassandraClient client = new SimpleCassandraClient();
    public RandomDataGenerator dataGenerator = new RandomDataGenerator( client );
    private final int rowCount = 1000;


    @Before
    public void initConnection() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping initConnection()" );
            return;
        }
        client.connect( getIPList( testCluster ) );
    }


    @Test
    public void test001_connectionTest() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping connectionTest()" );
            return;
        }
        assertNotNull( client.getSession() );
        assertNotNull( client.getSession().getCluster() );
        Metadata metadata = client.getSession().getCluster().getMetadata();
        assertNotNull( metadata );
    }


    @Test
    public void test002_dataLoadTest() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping checkSchema()" );
            return;
        }
        dataGenerator.createTestKeyspaceIfNotExists();
        dataGenerator.createTestTablesIfNotExists();
        dataGenerator.generateRandomData( rowCount );
    }


    @After
    public void closeConnection() {
        if( testCluster == null ) {
            LOG.info( "Test cluster is null, skipping closeConnection()" );
            return;
        }
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
