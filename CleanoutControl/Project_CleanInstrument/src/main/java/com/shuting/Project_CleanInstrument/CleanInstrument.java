package com.shuting.Project_CleanInstrument;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CleanInstrument {

	// MyMapper函数
	public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("filePath");
			ConfigInf.initOrgMap(filePath);			
		}

		public void map(ImmutableBytesWritable row, Result values, Context context)
				throws IOException, InterruptedException {

			String instruID = Bytes.toString(row.get());
			String instruName = "";
			String instruOrg = "";
			String remark1 = "";
			String instruPerson = "";
			ArrayList<ImmutableBytesWritable> valueSet = new ArrayList<ImmutableBytesWritable>();
			
			String valueStr1 = "instruID<=>" + instruID;
			ImmutableBytesWritable value1 = new ImmutableBytesWritable(Bytes.toBytes(valueStr1));
			valueSet.add(value1);
			Cell rawCell[] = values.rawCells();
			for (Cell cell : rawCell) {
				if ("m_instru_c_h_n_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					instruName = Bytes.toString(CellUtil.cloneValue(cell));
					DataCleaning cleanData=new DataCleaning(instruName,"01_（_(#01_）_)#01_\\?_#11");	    				
					if(cleanData.cleanDataByRule()){          	      					 	       				
						instruName=cleanData.getCleanResult();  
	       			}   			
				} else if ("m_instru_location".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					instruOrg = Bytes.toString(CellUtil.cloneValue(cell));
					DataCleaning cleanData=new DataCleaning(instruOrg,"01_（_(#01_）_)#04_(_)#01_\\?_#11#10");	    				
	    			if(cleanData.cleanDataByRule()){          	      					 	       				
						String result=cleanData.getCleanResult();
						instruOrg=result.length()>0?result:"无";
	       			} 				
				} else if ("remark1".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					remark1 = Bytes.toString(CellUtil.cloneValue(cell));
					DataCleaning cleanData=new DataCleaning(remark1,"01_（_(#01_）_)#04_(_)#01_\\?_#11#10");	    				
					if(cleanData.cleanDataByRule()){          	      					 	       				
						remark1=cleanData.getCleanResult(); 
	       			}	
				} else if ("m_instru_contact_person".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					String person = Bytes.toString(CellUtil.cloneValue(cell));
					if (person.equals("不详") || person.equals("暂无填写") || person.equals("暂时无填") || person.equals("暂时未填")
							|| person.isEmpty()) {
						instruPerson = "无";				
					}   
					else {						
						if (person.equals("朱毅马世杰")) {
							person = "朱毅;马世杰";
						}
						DataCleaning cleanData=new DataCleaning(person,"01_ +_;#01_（_(#01_）_)#04_(_)#13");	    				
						if(cleanData.cleanDataByRule()){ 						
							String result=cleanData.getCleanResult();
							//存在清洗的结果为""的情况导致
							//仪器表中可能没有联系人或组织信息 EG:8a807cae5c757f72015c778297a10351
							//更新人才库出现错位:EG：->陕西陕航环境试验有限公司
							instruPerson=result.length()>0?result:"无";
						}
					}					
				} 
				else if ("m_instru_organization".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					continue;
				}				
				else {
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String cellValue = Bytes.toString(CellUtil.cloneValue(cell));					
					DataCleaning cleanData=new DataCleaning(cellValue,"11#01_\\?_");	    				
					if(!cleanData.cleanDataByRule()){          	      					 	       				
						continue;
	       			}
					cellValue=cleanData.getCleanResult();
					if(!cellValue.isEmpty()){
						String valueStr = qualifier + "<=>" +cellValue ;
						ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
						valueSet.add(value);
					}					
				}
			}
			if (instruName.isEmpty() || instruID.equals("8a80a294598cfbf601598d11d6770004") || instruID.equals("ff80808159b9422d0159bab37dd600a4") ||
					instruID.equals("8a807cae5b69df24015b6b4536d80031") || instruID.equals("8a807cae5ba2ab7b015ba39acf690006")) {
				return;
			}
			//没有仪器联系人的字段
			if(instruPerson.isEmpty()){
				instruPerson="无";								
			}
			String valueStr = "m_instru_contact_person<=>" + instruPerson;
			ImmutableBytesWritable personValue = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
			valueSet.add(personValue);
			
			if (instruOrg.isEmpty()) {				
				if (!remark1.isEmpty()) {
					instruOrg = remark1;
				} else if (instruPerson.equals("李洋")) {
					instruOrg = "陕西省电子信息产品监督检验院";
				} else {
					instruOrg = "无";
				}
			}			
			String keyStr = instruName + "->" + instruOrg;
			ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(keyStr));
			for (int i = 0; i < valueSet.size(); i++) {
				ImmutableBytesWritable value = valueSet.get(i);
				context.write(key, value);
			}
		}
	}

	// MyReduce函数
	public static class MyReducer
			extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {

			String instruName = Bytes.toString(key.get()).split("->")[0];
			String instruOrg = Bytes.toString(key.get()).split("->")[1];

			ArrayList<String> qualifierSet = new ArrayList<String>();
			ArrayList<String> valueSet = new ArrayList<String>();
			String instruID = "";
			for (ImmutableBytesWritable val : values) {
				String valueStr = Bytes.toString(val.get());
				String qualifier = valueStr.split("<=>")[0];
				String value = valueStr.split("<=>")[1];
				if (qualifier.equals("instruID")) {
					instruID = value;
					continue;
				}
				if (!qualifierSet.contains(qualifier)) {
					qualifierSet.add(qualifier);
					valueSet.add(value);
				}
			}

			Put put = new Put(Bytes.toBytes(instruID));
			// 1——插入仪器名
			put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("m_instru_c_h_n_name"), Bytes.toBytes(instruName));
			context.write(null, put);
			// 2——插入仪器组织
			put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes("m_instru_organization"), Bytes.toBytes(instruOrg));
			context.write(null, put);
			for (int i = 0; i < qualifierSet.size(); i++) {
				put.addColumn(Bytes.toBytes("inf"), Bytes.toBytes(qualifierSet.get(i)), Bytes.toBytes(valueSet.get(i)));
				context.write(null, put);
			}
		}
	}

	// 主函数
	public static void main(String[] args) throws Exception {

		WriteRunLog.writeToFiles("正在运行的是: clean_instru.jar ......");
		if (args.length != 3) {
			WriteRunLog.writeToFiles("抱歉！您没有输入正确的参数个数(3)");
			System.exit(0);
		}
		String inputTable = args[0];
		String outputTable = args[1];
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("filePath", args[2]);
		Job job = Job.getInstance(hbaseConf, "clean_instru");
		job.setJarByClass(CleanInstrument.class);
		
		ConfigInf.initOrgMap(args[2]);
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(inputTable, scan, MyMapper.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(outputTable, MyReducer.class, job);

		job.waitForCompletion(true);
		WriteRunLog.writeToFiles("clean_instru.jar 运行成功！！！");
	}

}
