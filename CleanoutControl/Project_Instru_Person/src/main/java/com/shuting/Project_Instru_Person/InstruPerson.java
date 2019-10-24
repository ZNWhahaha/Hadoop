package com.shuting.Project_Instru_Person;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class InstruPerson {

	public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		public void map(ImmutableBytesWritable row, Result values, Context context)
				throws IOException, InterruptedException {

			String rowkey = Bytes.toString(row.get());
			// 信息来自人才表
			if (rowkey.contains("->")) {
				String personName = rowkey.split("->")[0];
				String orgName = rowkey.split("->")[1];
				ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(personName));
				ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(orgName));
				context.write(key, value);
			}
			// 信息来自仪器表
			else {
				String instruID2 = rowkey;
				String instruID="";
				String instruName = "";
				String instruOrg = "";
				String conPerson = "";
				Cell rawCell[] = values.rawCells();
				for (Cell cell : rawCell) {					
					if ("instruID".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						instruID = Bytes.toString(CellUtil.cloneValue(cell));
					}					
					else if ("m_instru_c_h_n_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						instruName = Bytes.toString(CellUtil.cloneValue(cell));
					}
					else if ("m_instru_contact_person".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						conPerson = Bytes.toString(CellUtil.cloneValue(cell));
						if (conPerson.equals("无")) {
							return;
						}
					}					
					else if ("m_instru_organization".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						instruOrg = Bytes.toString(CellUtil.cloneValue(cell));
						if (instruOrg.equals("无")) {
							return;
						}
					}
				}
				
				String valueStr = instruOrg + "<=>" + instruID+"_"+instruID2+ "=>" + instruName + "->" + instruOrg;
				ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
				String[] conPersonSet = conPerson.split(";");
				for (int i = 0; i < conPersonSet.length; i++) {
					String keyStr = conPersonSet[i];
					ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(keyStr));
					context.write(key, value);
				}
			}
		}
	}

	public static class MyReducer
			extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {

			String nameCH = Bytes.toString(key.get());
			// 存储人才的组织信息
			ArrayList<String> personOrgSet = new ArrayList<String>();
			// 存储仪器联系人信息
			ArrayList<String> instruOrgSet = new ArrayList<String>();
			ArrayList<String> instruInfSet = new ArrayList<String>();

			for (ImmutableBytesWritable val : values) {
				String valueStr = Bytes.toString(val.get());
				if (valueStr.contains("<=>")) {
					String[] splitStr = valueStr.split("<=>");
					if (!instruOrgSet.contains(splitStr[0])) {
						instruOrgSet.add(splitStr[0]);
						instruInfSet.add(splitStr[1]);
					} else {
						int index = instruOrgSet.indexOf(splitStr[0]);
						String instruStr = instruInfSet.get(index);
						instruStr = instruStr + "<==>" + splitStr[1];
						instruInfSet.set(index, instruStr);
					}
				} else {
					personOrgSet.add(valueStr);
				}
			}

			if (instruOrgSet.isEmpty()) {
				return;
			}

			for (int i = 0; i < instruOrgSet.size(); i++) {
				String orgName = instruOrgSet.get(i);
				Boolean isExist = false;
				int personIndex = 0;
				for (int j = 0; j < personOrgSet.size(); j++) {
					if (personOrgSet.get(j).contains(orgName)) {
						isExist = true;
						personIndex = j;
						break;
					}
				}
				if (isExist) {
					String keyStr = nameCH + "->" + personOrgSet.get(personIndex);
					Put put = new Put(keyStr.getBytes());

					String[] instruSet = instruInfSet.get(i).split("<==>");
					for (int k = 0; k < instruSet.length; k++) {
						String instruID = instruSet[k].split("=>")[0];
						String instruInf = instruSet[k].split("=>")[1];
						put.addColumn(Bytes.toBytes("instru"), Bytes.toBytes(instruID), Bytes.toBytes(instruInf));
						context.write(null, put);
					}
				} else {
					String keyStr = nameCH + "->" + instruOrgSet.get(i);
					Put put = new Put(keyStr.getBytes());

					String[] instruSet = instruInfSet.get(i).split("<==>");
					for (int k = 0; k < instruSet.length; k++) {
						String instruID = instruSet[k].split("=>")[0];
						String instruInf = instruSet[k].split("=>")[1];
						put.addColumn(Bytes.toBytes("instru"), Bytes.toBytes(instruID), Bytes.toBytes(instruInf));
						context.write(null, put);
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {

		WriteRunLog.writeToFiles("正在运行的是: instru_person.jar ......");
		if (args.length != 2) {
			WriteRunLog.writeToFiles("抱歉！您没有输入正确的参数个数（2）");
			System.exit(0);
		}
		String inputTable = args[0];
		String outputTable = args[1];
		Configuration hbaseConf = HBaseConfiguration.create();
		Job job = Job.getInstance(hbaseConf, "instru_person");
		job.setJarByClass(InstruPerson.class);

		List scans = new ArrayList();
		Scan scan1 = new Scan();
		scan1.setCaching(500);
		scan1.setCacheBlocks(false);
		scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(inputTable));
		scans.add(scan1);

		Scan scan2 = new Scan();
		scan2.setCaching(500);
		scan2.setCacheBlocks(false);
		scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(outputTable));
		scans.add(scan2);

		TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(outputTable, MyReducer.class, job);

		// 必备！！！(缺失会报错：无法加载主类！！！)
		job.waitForCompletion(true);
		
		WriteRunLog.writeToFiles("instru_person.jar 运行成功！！！");
		
	}
}
