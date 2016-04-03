package org.mdicreatives.Hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
//import java.net.URI;
//import java.lang.String;
//import java.util.*;

public class HDFSFileOperation {
	//Class Members
	
	public FileSystem hdfs;
	//public Path newfolderpath;
	FsShell shell;
	
	//Constructor Start
	public  HDFSFileOperation() throws IOException
	{  
		// Conf object will read the HDFS configuration parameters from these XML
		Configuration conf= new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		conf.set("fs.defaultFS","hdfs://hadoopmaster:54310");
		
		//Creating the FileSystem object.
		 hdfs =FileSystem.get(conf);
		 shell=new FsShell(conf);
			
	}//Constructor End
	
	//gives current Directory
	
	
	//Function Start
	/*public String getCurrentDirectory()throws IOException
	{
		return newfolderpath.toString();
	}//Function End */
	
	public void setpath(Path newpath)throws IOException
	{
		Path homepath = hdfs.getWorkingDirectory();
		System.out.println("Homepath-"+homepath);
		//newfolderpath = Path.mergePaths(homepath,newpath);
	}
	
	//function start
	public void makedir(String dir) throws IOException
	{
		
		Path path= new Path(""+dir);
	 
		
		if(hdfs.exists(path))
		{
		//Delete existing Directory
		System.out.println("Directory Already Exists");
		hdfs.delete(path, true);
		System.out.println("Existing Directory Deleted.");
		}
		System.out.println("Creating New Directory");
		hdfs.mkdirs(path);     //Create new Directory
		System.out.println("Directory Created.");
		
		
	}//function end
	
	//function start
	public void copyfilestoHDFS(String filename,String localpath, String hdfspath) throws IOException
	{
		
		
	      try {
	        shell.run(new String[]{"-chmod","-R","777",hdfspath});
	      }
	     catch (  Exception e) {
	        System.out.println("Couldnt change the file permissions ");
	        throw new IOException(e);
	      }
		Path localFilePath = new Path(localpath+ "/"+ filename);
		System.out.println(localFilePath);

		//Path hdfsFilePath= new Path(newfolderpath.toString()+"/"+ filename);
		//System.out.println(hdfsFilePath);

		hdfs.copyFromLocalFile(localFilePath,new Path(hdfspath));
		

		System.out.println("File copied from local to HDFS.");
		hdfs.close();
		
	}//function end
	
}
