package org.apache.usergrid.chop.example.subutai.hadoop;


import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SimpleHadoopClient implements HadoopClient {

    private static final Logger LOG = LoggerFactory.getLogger( SimpleHadoopClient.class );

    public static final int DEFAULT_NAMENODE_PORT = 8020;
    public static final int DEFAULT_JOBTRACKER_PORT = 9000;

    private String nameNodeIp;
    private String jobTrackerIp;
    private String userName;
    private Configuration conf;
    private int nameNodePort = DEFAULT_NAMENODE_PORT;
    private int jobTrackerPort = DEFAULT_JOBTRACKER_PORT;


    public SimpleHadoopClient( String nameNodeIp, String jobTrackerIp, String userName ) {
        this.nameNodeIp = nameNodeIp;
        this.jobTrackerIp = jobTrackerIp;
        this.userName = userName;
        configure();
    }


    public void configure() {
        conf = new Configuration();
        conf.set( "fs.default.name",
                new StringBuilder( "hdfs://" ).append( nameNodeIp ).append( ":" ).append( nameNodePort ).toString() );
        conf.set( "mapred.job.tracker",
                new StringBuilder( jobTrackerIp ).append( ":" ).append( jobTrackerPort ).toString() );
        System.setProperty( "HADOOP_USER_NAME", userName );
    }


    public void setConf( final Configuration conf ) {
        this.conf = conf;
    }


    public int getNameNodePort() {

        return nameNodePort;
    }


    public void setNameNodePort( final int nameNodePort ) {
        this.nameNodePort = nameNodePort;
    }


    public int getJobTrackerPort() {
        return jobTrackerPort;
    }


    public void setJobTrackerPort( final int jobTrackerPort ) {
        this.jobTrackerPort = jobTrackerPort;
    }


    public String getNameNodeIp() {
        return nameNodeIp;
    }


    public String getJobTrackerIp() {
        return jobTrackerIp;
    }


    public String getUserName() {
        return userName;
    }


    public Configuration getConf() {
        return conf;
    }


    @Override
    public boolean createDirectoryOnHDFS( final String directoryPath ) {
        boolean isCreated = false;
        try {
            FileSystem fs = FileSystem.get( conf );
            isCreated = fs.mkdirs( new Path( directoryPath ) );
        }
        catch ( IOException e ) {
            LOG.error( "Error while initializing the filesystem: {}", e );
        }
        return isCreated;
    }
}
