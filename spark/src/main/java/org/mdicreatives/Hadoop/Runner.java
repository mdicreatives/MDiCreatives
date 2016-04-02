package org.mdicreatives.Hadoop;
import org.mdicreatives.Hadoop.HDFSFileOperation;
import org.apache.hadoop.fs.*;

import java.io.*;

public class Runner {
	 
	public static String mypath= "/DistributedAnalytics";
	public static String forfile= "";
	public static String localfile = "/home/mdanish/BigData/Distributed Analytics of Machine Data/Code"; //local file path here
	public static String filename = "pom.xml"; //filename here
	 
	public static void main(String[] args) throws IOException 
	{
		
		HDFSFileOperation hd= new HDFSFileOperation();
		//hd.makedir(mypath);
		
		Path newpath= (new Path(mypath));
		hd.setpath(newpath);
		//forfile=hd.getCurrentDirectory();
		hd.copyfilestoHDFS(filename,localfile);
		
		System.out.println("Runner Done");
		
		
	}

}
