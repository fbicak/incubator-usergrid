package org.apache.usergrid.chop.example.subutai.hadoop;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.UUID;

import org.fluttercode.datafactory.impl.DataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HadoopTestDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger( HadoopTestDataGenerator.class );

    private static DataFactory dataFactory = new DataFactory();


    public static String generateTestData( final String destinationPath, final int rowCount ) {
        String testFileName = new StringBuilder( HadoopTestDataGenerator.class.getSimpleName() ).append( "-testfile-" )
                .append( UUID.randomUUID() ).toString();
        createDirectoryIfNotExists( destinationPath );
        String testFileFullPath = new StringBuilder( destinationPath ).append( "/" ).append( testFileName ).toString();
        PrintWriter out;
        try {
            LOG.info( "Generating test data on {} file.", testFileName );
            out = new PrintWriter( new BufferedWriter( new FileWriter( testFileFullPath, true ) ) );
            String randomText;
            for ( int i = 0; i < rowCount; i++ ) {
                randomText = dataFactory.getRandomText( 100, 1000 );
                out.println( randomText );
            }
            out.close();
        } catch ( Exception e ) {
            LOG.error( "An error occurred while writing data to {} file: {}", testFileName, e );
        }
        LOG.info( "Test data is generated successfully on {} file.", testFileName );
        return testFileName;
    }


    private static void createDirectoryIfNotExists( final String directoryPath ) {
        File directory = new File( directoryPath );
        // Create the directory if not exists
        if ( ! directory.exists() ) {
            LOG.info( "Creating directory: {}", directory.getAbsolutePath() );
            try {
                directory.mkdir();
            }
            catch( SecurityException e ){
                LOG.error( "An error occurred while creating the directory: {}", e );
                return;
            }
            LOG.info("Directory {} created.", directory.getAbsolutePath() );
        }
        else {
            LOG.debug( "Directory {} already exists, not creating it", directory.getAbsolutePath() );
        }
    }

}
