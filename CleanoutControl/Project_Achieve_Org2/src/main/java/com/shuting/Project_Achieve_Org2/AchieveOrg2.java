package com.shuting.Project_Achieve_Org2;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AchieveOrg2 {

	public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String filePath = conf.get("filePath");
			ConfigInf.initOrgMap(filePath);			
		}
		
		public void map(ImmutableBytesWritable row, Result values, Context context)
				throws IOException, InterruptedException {

			String achieveID2=Bytes.toString(row.get()); 
			String achieveID = "";
			String achieveName = "";
			String personStr = "";
			String orgStr = "";

			String jibie = "";
			String dengji = "";
			String leixing = "";
			String year = "";

			Cell rawCell[] = values.rawCells();
			for (Cell cell : rawCell) {
				if ("project_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					// 与成果登记库的处理一致
					achieveName = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					achieveName=achieveName.replace("（", "(");
     	    		achieveName=achieveName.replace("）", ")");
     	    		
     	    		achieveName=achieveName.replace("“","\"");
     	    		achieveName=achieveName.replace("”","\"");
     	    		
     	    		achieveName=achieveName.replace("，",",");	
				}
				else if ("achieveID".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					achieveID = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
				}
				else if ("main_people".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					personStr = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					personStr = personStr.replace("（", "(");
					personStr = personStr.replace("）", ")");
				} 
				else if ("main_unit".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					orgStr = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					orgStr = orgStr.replace("（", "(");
					orgStr = orgStr.replace("）", ")");
				} 
				else if ("province_or_country".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					jibie = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
				} 
				else if ("reward_level".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					dengji = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
				} 
				else if ("reward_type".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					leixing = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
				} 
				else if ("year".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					year = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
				}
			}
			if (jibie.isEmpty()) {
				jibie = "暂无填写";
			}
			ArrayList<String> orgSet = new ArrayList<String>();
			if (!orgStr.isEmpty()) {
				// 形式：中国石油天然气股份有限公司长庆油田分公司、中国石油大学（北京）、中国石油天然气股份有限公司勘探开发研究院
				String[] splitStr = orgStr.split("、");
				for (int i = 0; i < splitStr.length; i++) {
					String tempStr = splitStr[i];
					String org = "";
					if (tempStr.contains("(")) {
						if (tempStr.contains(")")) {
							int left = tempStr.indexOf("(");
							int right = tempStr.indexOf(")");
							String subStr = tempStr.substring(left, right + 1);
							org = tempStr.replace(subStr, "");
						} 
						else {
							int left = tempStr.indexOf("(");
							org = tempStr.substring(0, left);
						}					
					} 
					else {
						org = tempStr;
					}					
					Organization orgClean=new Organization(org);
	   	    		org=orgClean.handleOrg();  
					orgSet.add(org);
				}
			}
			else{

				String[] splitStr2=personStr.split("、");		            
				for(int i=0;i<splitStr2.length;i++){	            	
		            	
					String tempStr=splitStr2[i];		            	
					String org="";		            	
					if (tempStr.contains("(") && tempStr.contains(")")) {		            		
						int left = tempStr.indexOf("(");
						int right = tempStr.lastIndexOf(")");							
          				org = tempStr.substring(left+1, right);		 		            		
          				if(org.length()>3){          					
          					if (org.contains("(") && org.contains(")")){              					
          						int left2 = tempStr.indexOf("(");              					
          						int right2 = tempStr.indexOf(")");           					
          						String subStr = tempStr.substring(left2, right2 + 1);
   								org = org.replace(subStr, "");	  	              		
          					} 
          					Organization orgClean=new Organization(org);
        	   	    		org=orgClean.handleOrg();  
          					orgSet.add(org);
           	            }		            	
					}
				}
			}		                
			
			String valueStr = achieveID+"=>"+achieveID2 + "->" + achieveName + "->" + jibie+";"+dengji+";"+leixing+";"+year;
			ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
			for (int i = 0; i < orgSet.size(); i++) {
				ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(orgSet.get(i)));
				context.write(key, value);
			}
		}
	}

	public static class MyReducer
			extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {
			
			Put put = new Put(key.get());
			for (ImmutableBytesWritable val : values) {

				String valueStr = Bytes.toString(val.get());
				String achieveID = valueStr.split("=>")[0];
				String cellValue = valueStr.split("=>")[1];
				put.addColumn(Bytes.toBytes("achieve"), Bytes.toBytes(achieveID), Bytes.toBytes(cellValue));
				context.write(null, put);
			}
		}
	}

	// 用成果奖励库更新组织
	public static void main(String[] args) throws Exception {
		
		WriteRunLog.writeToFiles("正在运行的是: achieveAward_org.jar ......");
		if (args.length != 3) {
			WriteRunLog.writeToFiles("抱歉！您没有输入正确参数(参数为:输入和输出的Hbase表名)");
			System.exit(0);
		}
		String inputTable = args[0];
		String outputTable = args[1];

		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("filePath", args[2]);
		Job job = Job.getInstance(hbaseConf, "achieveAward_org");
		job.setJarByClass(AchieveOrg2.class);
		
		ConfigInf.initOrgMap(args[2]);
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(inputTable, scan, MyMapper.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(outputTable, MyReducer.class, job);

		// 必备！！！(缺失会报错：无法加载主类！！！)
		job.waitForCompletion(true);

		WriteRunLog.writeToFiles("achieveAward_org.jar 运行成功！！！");
	}
}
