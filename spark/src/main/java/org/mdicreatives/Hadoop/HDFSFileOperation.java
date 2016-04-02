package org.mdicreatives.Hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.*;
//import java.lang.String;
//import java.util.*;

public class HDFSFileOperation {
	//Class Members
	private Path workingDir;
	private Path newFolderPath;
	private FileSystem hdfs;
	
	//Constructor Start
	public  HDFSFileOperation() throws IOException
	{
		FileSystem hdfs =FileSystem.get(new Configuration());
		System.out.println("Home folder - " +  hdfs.getHomeDirectory());
			
	}//Constructor End
	
	//function start
	public void makedir() throws IOException
	{
		//Current Working Directory 
	    workingDir= hdfs.getWorkingDirectory();
		newFolderPath= new Path("/MyDataFolder");
		newFolderPath=Path.mergePaths(workingDir, newFolderPath);
		
		if(hdfs.exists(newFolderPath))
		{

		//Delete existing Directory

		hdfs.delete(newFolderPath, true);

		System.out.println("Existing Folder Deleted.");

		hdfs.mkdirs(newFolderPath);     //Create new Directory

		System.out.println("Folder Created.");
		}
		
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
