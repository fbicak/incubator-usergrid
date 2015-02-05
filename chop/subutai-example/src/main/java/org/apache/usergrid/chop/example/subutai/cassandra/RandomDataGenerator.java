package org.apache.usergrid.chop.example.subutai.cassandra;


import java.util.UUID;

import org.fluttercode.datafactory.impl.DataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class RandomDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger( RandomDataGenerator.class );

    private static final String TEST_KEYSPACE_NAME = "test_keyspace";
    private static final String TEST_TABLE_NAME = "test_table";
    CassandraClient client;
    DataFactory dataFactory;


    public RandomDataGenerator( CassandraClient client ) {
        this.client = client;
        dataFactory = new DataFactory();
    }


    public void createTestKeyspaceIfNotExists() {
        Preconditions.checkNotNull( client.getSession() );
        LOG.info( "Creating keyspace {} if not exists already", TEST_KEYSPACE_NAME );
        client.getSession().execute( "CREATE KEYSPACE IF NOT EXISTS " + TEST_KEYSPACE_NAME + " WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':3};" );
    }


    public void createTestTablesIfNotExists() {
        Preconditions.checkNotNull( client.getSession() );
        LOG.info( "Creating table {} on {} keyspace if not exists already", TEST_TABLE_NAME, TEST_KEYSPACE_NAME );
        client.getSession().execute( "CREATE TABLE IF NOT EXISTS " +
                        TEST_KEYSPACE_NAME +
                        "." +
                        TEST_TABLE_NAME +
                        "(" +
                        "id uuid PRIMARY KEY," +
                        "random_text text" +
                        ");" );
    }


    public void generateRandomData( int rowCount ) {
        Preconditions.checkNotNull( client.getSession() );
        LOG.info( "Generating random data({}) on {} table", rowCount, TEST_TABLE_NAME );
        int waitTimeInMilliSeconds = 50;
        int consecutiveEntryCount = 100;
        for ( int i = 0; i < rowCount; i++ ) {
            // Wait some small time after each consecutive 100 entry to simulate real usage

            if ( i % consecutiveEntryCount == 0 ) {
                try {
                    LOG.info( "Waiting {} milliseconds as {} consecutive entry is written..."
                            , waitTimeInMilliSeconds, consecutiveEntryCount );
                    Thread.sleep( waitTimeInMilliSeconds );
                }
                catch ( InterruptedException e ) {
                    e.printStackTrace();
                }
            }

            client.getSession().execute(
                    "INSERT INTO " + TEST_KEYSPACE_NAME + "." + TEST_TABLE_NAME + " (id, random_text) " +
                            "VALUES (" +
                            UUID.randomUUID() +
                            "," +
                            "'" + dataFactory.getRandomText( 100, 1000 ) + "'" +
                            ");" );
        }
    }

}
