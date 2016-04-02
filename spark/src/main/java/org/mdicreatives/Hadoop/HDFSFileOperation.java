package org.mdicreatives.Hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.net.URI;
//import java.lang.String;
//import java.util.*;

public class HDFSFileOperation {
	//Class Members
	public Path workingDir;
	public Path newFolderPath;
	public FileSystem hdfs;
	public Path homeDir;
	
	//Constructor Start
	public  HDFSFileOperation() throws IOException
	{
		Configuration conf= new Configuration();
		conf.addResource(new Path("/usr/local/hadoop/conf/core-site.xml"));
	    conf.addResource(new Path("/usr/local/hadoop/conf/hdfs-site.xml"));
		//conf.set("fs.defaultFS","hdfs://hadoopmaster:54310");
		FileSystem hdfs =FileSystem.get(conf);
		
		 homeDir=hdfs.getHomeDirectory();
		System.out.println("Home folder - " +  hdfs.getHomeDirectory());
			
	}//Constructor End
	
	//function start
	public void makedir(String dir) throws IOException
	{
		//Current Working Directory 
		Path path= new Path(dir);
	  // workingDir= hdfs.getWorkingDirectory();
		//newFolderPath= new Path("/MyDataFolder");
		//newFolderPath=Path.mergePaths(workingDir, newFolderPath);
		
		/*if(hdfs.exists(newFolderPath))
		{

		//Delete existing Directory

		hdfs.delete(newFolderPath, true);

		System.out.println("Existing Folder Deleted.");*/

		hdfs.mkdirs(path);     //Create new Directory

		System.out.println("Folder Created.");
		//}
		
	}//function end
	
	//function start
	public void copyfilestoHDFS() throws IOException
	{
		Path localFilePath = new Path("c://localdata/datafile1.txt");

		Path hdfsFilePath= new Path(newFolderPath+"/dataFile1.txt");

		hdfs.copyFromLocalFile(localFilePath, hdfsFilePath);

		System.out.println("File copied from local to HDFS.");
		
	}//function end
	
}
