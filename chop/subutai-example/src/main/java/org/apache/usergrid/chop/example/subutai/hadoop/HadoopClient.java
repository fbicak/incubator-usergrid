package org.apache.usergrid.chop.example.subutai.hadoop;


import org.apache.hadoop.conf.Configuration;


public interface HadoopClient {

    public void configure();


    public void setConf( final Configuration conf );


    public int getNameNodePort();


    public void setNameNodePort( final int nameNodePort );


    public int getJobTrackerPort();


    public void setJobTrackerPort( final int jobTrackerPort );


    public String getNameNodeIp();


    public String getJobTrackerIp();


    public String getUserName();


    public Configuration getConf();


    /**
     * Creates directory on HDFS if the directory does not exist
     * @param directoryPath path of the directory on HDFS to be created
     * @return
     */
    public boolean createDirectoryOnHDFS( String directoryPath );


}
