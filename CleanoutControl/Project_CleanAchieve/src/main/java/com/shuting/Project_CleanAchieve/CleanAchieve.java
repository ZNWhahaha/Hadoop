package com.shuting.Project_CleanAchieve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class CleanAchieve {

	public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String filePath = conf.get("filePath");
			ConfigInf.initOrgMap(filePath);			
		}		
		
		public void map(ImmutableBytesWritable row, Result values, Context context)
				throws IOException, InterruptedException {
			
			String rowkey = Bytes.toString(row.get());			
			ArrayList<ImmutableBytesWritable> valueSet = new ArrayList<ImmutableBytesWritable>();

			Cell rawCell[] = values.rawCells();
			for (Cell cell : rawCell) {

				if ("result_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {

					String achieveName = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll(" ", "");
					if (achieveName.charAt(0) == '\'') {
						achieveName = achieveName.substring(1);
					}
					achieveName = achieveName.replaceAll("（", "(");
					achieveName = achieveName.replaceAll("）", ")");					
					achieveName = achieveName.replaceAll("，", ",");
					achieveName = achieveName.replaceAll("<SUB>", "<sub>");
					achieveName = achieveName.replaceAll("</SUB>", "</sub>");					
					String valueStr = "result_name<=>" + achieveName;
					ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
					valueSet.add(value);				
				} 
				else if ("unit_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {				
					
					String orgName = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll(" ", "");					
					orgName = orgName.replaceAll("'", "");
					String[] splitStr=orgName.split(";");
					String newOrgName="";
					for(String str : splitStr){
						Organization cleanOrg = new Organization(str);
						newOrgName += (newOrgName.length() > 0 ? ";" : "") + cleanOrg.handleOrg();
					}					
					orgName = newOrgName;
					orgName = orgName.replaceAll("（", "(");
					orgName = orgName.replaceAll("）", ")");					
					if (orgName.length() < 4) {
						orgName="无";
					}
					else{				
						if (orgName.contains("(")) { // inf : 单位名称 陕西省科技资源统筹中心（陕西省生产力促进中心							
							if (orgName.contains(")")) {
								int left = orgName.indexOf("(");
								int right = orgName.indexOf(")");
								String subStr = orgName.substring(left, right + 1);
								orgName = orgName.replace(subStr, "");
							} 
							else {
								int left = orgName.indexOf("(");
								orgName = orgName.substring(0, left);
							}
						}						
						if(orgName.contains(";") && orgName.split(";")[0].length()<4){							
							orgName="无";						
						}						
						if(orgName.contains("孙喜庆") || orgName.contains("同延龄")||orgName.contains("张占胜") || orgName.contains("姜宾")|| orgName.contains("池延斌")||orgName.contains("秦禄田") || orgName.contains("黄威权")){
							orgName="无";							
						}
					}
					String valueStr = "unit_name<=>" + orgName;
					ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
					valueSet.add(value);
				} 
				else if ("contacts".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {

					String conPerson = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("  ", "");
					conPerson = conPerson.replaceAll("'", "");
					conPerson = conPerson.replaceAll("，", "");
					
					if(conPerson.length() < 2 || conPerson.length() > 4 ){
						if(!conPerson.contains(";")){	
							conPerson = "无";							
						}					
					}
					else{						
						//建议写入配置文件或引入字典！！！
						String filterStr="办公室;业务科;科研科;科教科;工研院;医务处;科技处;科研处;科技部;行政部;生技部;技术中心;研发中心;油脂二部";						
						if(filterStr.contains(conPerson)){
							conPerson = "无";
						}				
					}					
					String valueStr = "contacts<=>" + conPerson;
					ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
					valueSet.add(value);					
				} 
				else {
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String colValue = Bytes.toString(CellUtil.cloneValue(cell));
					Pattern p = Pattern.compile("[\u4e00-\u9fa5_a-zA-Z0-9]");
					Matcher m = p.matcher(colValue);
					if (!m.find()) {
						continue;
					}
					colValue = colValue.replace("（", "(");
					colValue = colValue.replace("）", ")");
					colValue = colValue.replace("，", ",");
					colValue = colValue.replace("<SUB>", "<sub>");
					colValue = colValue.replace("</SUB>", "</sub>");

					String valueStr = qualifier + "<=>" + colValue;
					ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
					valueSet.add(value);
				}
			}
			ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(rowkey));
			for (int i = 0; i < valueSet.size(); i++) {
				ImmutableBytesWritable value = valueSet.get(i);
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
				String qualifier = valueStr.split("<=>")[0];
				String value = valueStr.split("<=>")[1];
				put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes(qualifier), Bytes.toBytes(value));
				context.write(null, put);
			}
		}
	}

	// 主函数
	public static void main(String[] args) throws Exception {

		WriteRunLog.writeToFiles("正在运行的是: cleanAchieve.jar ......");
		if (args.length != 3) {
			WriteRunLog.writeToFiles("抱歉！您没有输入正确的参数个数(3)");
			System.exit(0);
		}
		//源数据表
		String achieveTable = args[0];
		//生成数据表
		String achieveTableNew = args[1];	
		
		Configuration hbaseConf = HBaseConfiguration.create();
		//组织映射文件路径（HDFS）
		hbaseConf.set("filePath", args[2]);
		Job job = Job.getInstance(hbaseConf, "CleanAchieve");
		job.setJarByClass(CleanAchieve.class);
		
		ConfigInf.initOrgMap(args[2]);		
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);	

		TableMapReduceUtil.initTableMapperJob(achieveTable,scan, MyMapper.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(achieveTableNew, MyReducer.class, job);

		job.waitForCompletion(true);
		
		WriteRunLog.writeToFiles("cleanAchieve.jar 运行成功！！！");
	}

}
