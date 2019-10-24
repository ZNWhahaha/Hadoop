package com.Hadoop.Project_Achieve_Hownet_Org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;

//用静态常量的形式存储配置文件的信息
public class ConfigInf implements Serializable{
	//处理组织缩写问题
	static ArrayList<String> orgSmallSet=null;
	static ArrayList<String> orgFullSet=null;
	
	/**
	public static void initOrgMap(String orgMapPath) {	

		orgSmallSet=new ArrayList<String>();
		orgFullSet=new ArrayList<String>();
		try {	        	
	          Scanner scan = new Scanner(new File(orgMapPath));  
	          while (scan.hasNextLine()) {
	        	  
	        	  String lineStr = scan.nextLine();	               
	        	  String[] splitStr=lineStr.split("<=>");				
	        	  orgSmallSet.add(splitStr[0]);					
	        	  orgFullSet.add(splitStr[1]);	               
	          }	          
	          scan.close();
	          
	        } catch (Exception e) {
	            e.printStackTrace();
	        }	
		
	}**/
	
	public static void initOrgMap(String orgMapPath) {

		orgSmallSet=new ArrayList<String>();
		orgFullSet=new ArrayList<String>();
		
		Configuration conf = new Configuration();
		try {		
			FileSystem fs = FileSystem.get(URI.create(orgMapPath), conf);
			FSDataInputStream fsr = fs.open(new Path(orgMapPath));
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			
			String lineStr="";			
			while ((lineStr = bufferedReader.readLine()) != null) {
				
				String[] splitStr=lineStr.split("<=>");
				orgSmallSet.add(splitStr[0]);
				orgFullSet.add(splitStr[1]);
			}
			bufferedReader.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
