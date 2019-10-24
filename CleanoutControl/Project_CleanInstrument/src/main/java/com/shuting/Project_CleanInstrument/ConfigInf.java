package com.shuting.Project_CleanInstrument;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//用静态常量的形式存储配置文件的信息
public class ConfigInf implements Serializable {
  
	static ArrayList<String> orgOldSet = null;
	static ArrayList<String> orgNewSet=null;

	public static void initOrgMap(String orgMapPath) {
		
		orgOldSet=new ArrayList<String>();
		orgNewSet=new ArrayList<String>();
		
		Configuration conf = new Configuration();
		try	{			
			FileSystem fs = FileSystem.get(URI.create(orgMapPath),conf);
			FSDataInputStream fsr = fs.open(new Path(orgMapPath));
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr));	
			String lineStr="";			
			while((lineStr=bufferedReader.readLine())!=null){				
				String[] splitStr=lineStr.split("<=>");				
	        	orgOldSet.add(splitStr[0]);					
	        	orgNewSet.add(splitStr[1]);
			}			
			bufferedReader.close();
					
		}catch (Exception e){			
			e.printStackTrace();
		}		
	}	
}
