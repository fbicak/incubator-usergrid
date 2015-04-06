package org.apache.usergrid.chop.example.hadoop;


import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.usergrid.chop.api.IterationChop;
import org.apache.usergrid.chop.example.subutai.hadoop.HadoopClient;
import org.apache.usergrid.chop.example.subutai.hadoop.HadoopTestDataGenerator;
import org.apache.usergrid.chop.example.subutai.hadoop.SimpleHadoopClient;
import org.apache.usergrid.chop.example.subutai.hadoop.mapreduce.WordCount;
import org.apache.usergrid.chop.stack.ChopCluster;
import org.apache.usergrid.chop.stack.ICoordinatedCluster;
import org.apache.usergrid.chop.stack.Instance;

import com.google.common.base.Preconditions;


@FixMethodOrder( MethodSorters.NAME_ASCENDING)
@IterationChop( iterations = 2, threads = 1 )
public class HadoopClusterTest {
    private static final Logger LOG = LoggerFactory.getLogger( HadoopClusterTest.class );
    private static final String DEFAULT_HADOOP_USERNAME = "root";
    private static final String MAPREDUCE_JAR_PATH = "/root/runner.jar";
    private static final String TEST_FILES_DIRECTORY_ON_TEST_MACHINE = "/tmp/chop-test/";
    private static String baseTestDirectoryOnHDFS;
    private static String baseOutputTestDirectoryOnHDFS;
    private static String baseInputTestDirectoryOnHDFS;
    private static String testfileName;
    private static String destinationFileFullPath;

    // Cluster name should be the same with the cluster defined inside stack.json file
    @ChopCluster( name = "TestHadoopCluster" )
    public static ICoordinatedCluster testHadoopCluster;

    public static HadoopClient client;
    private static final int rowCount = 100000;


    @BeforeClass
    public static void init() {
        if ( ! clusterExists( testHadoopCluster ) ) {
            LOG.warn( "Could not find the cluster, returning!" );
            return;
        }
        ArrayList<String> ipList = getIPList( testHadoopCluster );
        String nameNodeIp = getNameNodeIp( ipList );
        String jobTrackerIp = getJobTrackerIp( ipList );
        client = new SimpleHadoopClient( nameNodeIp, jobTrackerIp, DEFAULT_HADOOP_USERNAME );
        Preconditions.checkNotNull( nameNodeIp, "NameNode ip address cannot be null!" );
        Preconditions.checkNotNull( jobTrackerIp, "JobTracker ip address cannot be null!" );
        baseTestDirectoryOnHDFS = new StringBuilder( "/user/" )
                .append( DEFAULT_HADOOP_USERNAME ).append( "/chop/" ).toString();
        baseInputTestDirectoryOnHDFS = baseTestDirectoryOnHDFS + "/input_files";
        baseOutputTestDirectoryOnHDFS = baseTestDirectoryOnHDFS + "/output_files";
        client.createDirectoryOnHDFS( baseTestDirectoryOnHDFS );
        testfileName = HadoopTestDataGenerator.generateTestData( TEST_FILES_DIRECTORY_ON_TEST_MACHINE, rowCount );
    }


    @Test
    public void test1_copyTestFilesToHDFS() throws IOException, ClassNotFoundException, InterruptedException {
        if ( ! clusterExists( testHadoopCluster ) ) {
            LOG.warn( "Could not find the cluster, returning!" );
            return;
        }
        Configuration configuration = client.getConf();
        FileSystem fs = FileSystem.get( configuration );
        destinationFileFullPath = new StringBuilder( baseInputTestDirectoryOnHDFS )
                .append( "/" ).append( testfileName ).toString();
        if( ! fs.exists( new Path( destinationFileFullPath ) ) ) {
            LOG.info( "File {} does not exist on HDFS, copying from local filesystem to HDFS.",
                    destinationFileFullPath );
            fs.copyFromLocalFile( false, false,
                    new Path( TEST_FILES_DIRECTORY_ON_TEST_MACHINE + "/" + testfileName ),new Path( destinationFileFullPath ) );
        }
        else {
            LOG.info( "File {} already exists on HDFS! Not copying it.", destinationFileFullPath );
        }
    }


    @Test
    public void test2_runMapReduceJob() throws Exception {
        if ( ! clusterExists( testHadoopCluster ) ) {
            LOG.warn( "Could not find the cluster, returning!" );
            return;
        }
        LOG.info( "Testing the hadoop cluster {} with a Mapreduce job!", testHadoopCluster.getName() );

        Configuration conf = client.getConf();
        conf.set( "mapred.jar", MAPREDUCE_JAR_PATH );
        Job job = new Job( conf, "Wordcount job for " + testfileName );

        job.setJarByClass( WordCount.class );

        job.setMapperClass( WordCount.Map.class );
        job.setReducerClass( WordCount.Reduce.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        job.setInputFormatClass( TextInputFormat.class );
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath( job, new Path( destinationFileFullPath ) );
        FileOutputFormat.setOutputPath( job, new Path( baseOutputTestDirectoryOnHDFS + "/" + testfileName ) );

        LOG.debug( "Job Jar: " + job.getJar() );
        boolean jobSuccess = job.waitForCompletion( true );
        if ( ! jobSuccess ) {
            throw new Exception( "Mapreduce job for " + testfileName + " failed!" );
        }
    }


    private static String getJobTrackerIp( final ArrayList<String> ipList ) {
        int jobTrackerPort = SimpleHadoopClient.DEFAULT_JOBTRACKER_PORT;
        String jobTrackerIp = null;
        for ( String  ip : ipList ) {
            boolean portOpen = isPortOpen( ip, jobTrackerPort );
            if ( portOpen ) {
                jobTrackerIp = ip;
                break;
            }
        }
        return jobTrackerIp;
    }


    private static String getNameNodeIp( final ArrayList<String> ipList ) {
        int nameNodePort = SimpleHadoopClient.DEFAULT_NAMENODE_PORT;
        String nameNodeIp = null;
        for ( String  ip : ipList ) {
            boolean portOpen = isPortOpen( ip, nameNodePort );
            if ( portOpen ) {
                nameNodeIp = ip;
                break;
            }
        }
        return nameNodeIp;
    }


    private static ArrayList<String> getIPList( ICoordinatedCluster cluster ){
        if( cluster == null ) {
            return new ArrayList<String>( 0 );
        }
        ArrayList<String> ipList = new ArrayList<String>();
        for ( Instance temp : cluster.getInstances() ) {
            ipList.add( temp.getPrivateIpAddress() );
        }
        return ipList;
    }


    private static boolean clusterExists( ICoordinatedCluster cluster ) {
        return cluster != null;
    }


    private static boolean isPortOpen( String ipAddress, int port ) {
        Socket socket = null;
        boolean reachable = false;
        try {
            socket = new Socket( ipAddress, port );
            reachable = true;
        }
        catch ( UnknownHostException e ) {
            LOG.debug( "Host {} is not reachable! Error: {}", ipAddress, e );
        }
        catch ( IOException e ) {
        }
        finally {
            if (socket != null) try { socket.close(); } catch(IOException e) {}
        }
        return reachable;
    }
}
