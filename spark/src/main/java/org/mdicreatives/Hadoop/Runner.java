package org.mdicreatives.Hadoop;
import org.mdicreatives.Hadoop.HDFSFileOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

public class Runner {
	
	public static void main(String[] args) throws IOException 
	{
		Configuration conf= new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		conf.set("fs.defaultFS","hdfs://hadoopmaster:54310");
		FileSystem hdfs =FileSystem.get(conf);
		//Path path = new Path("hdfs://localhost:54310/user/hduser/new");
		Path path = new Path("/user/hduser/new");
		hdfs.mkdirs(path);

        hdfs.close();
		//HDFSFileOperation hd= new HDFSFileOperation();
		//hd.makedir("hdfs://hadoopmaster:54310/user/hduser/new");
		System.out.println("Runner Completed");//debugger
		
	}

}
