package com.shuting.Project_Instru_Org;

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

public class InstruOrg {

	public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		public void map(ImmutableBytesWritable row, Result values, Context context)
				throws IOException, InterruptedException {

			String instruID2 = Bytes.toString(row.get());
			String instruID="";
			String instruName = "";
			String orgName = "";

			Cell rawCell[] = values.rawCells();
			for (Cell cell : rawCell) {
				
				if ("instruID".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					instruID = Bytes.toString(CellUtil.cloneValue(cell));
				} 
				else if ("m_instru_c_h_n_name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					instruName = Bytes.toString(CellUtil.cloneValue(cell));
				} 
				else if ("m_instru_organization".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					orgName = Bytes.toString(CellUtil.cloneValue(cell));
					if (orgName.equals("无")) {
						return;
					}
				}
			}
			
			//仪器表中可能没有组织信息 EG:8a807cae5c757f72015c778297a10351
			if(orgName.isEmpty()){
				return;
			}
			ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(orgName));
			String valueStr = instruID+"_"+instruID2+"=>" + instruName;
			ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
			context.write(key, value);
		}
	}

	public static class MyReducer
			extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

		public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {

			Put put = new Put(key.get());

			for (ImmutableBytesWritable val : values) {
				String valueStr = Bytes.toString(val.get());
				String instruID = valueStr.split("=>")[0];
				String instruName = valueStr.split("=>")[1];
				put.addColumn(Bytes.toBytes("instru"), Bytes.toBytes(instruID), Bytes.toBytes(instruName));
				context.write(null, put);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		WriteRunLog.writeToFiles("正在运行的是: instru_org.jar ......");
		if (args.length != 2) {
			WriteRunLog.writeToFiles("抱歉！您没有输入正确的参数个数（2）");
			System.exit(0);
		}
		String inputTable = args[0];
		String outputTable = args[1];
		
		Configuration hbaseConf = HBaseConfiguration.create();
		Job job = Job.getInstance(hbaseConf, "instru_org");
		job.setJarByClass(InstruOrg.class);
		
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(inputTable, scan, MyMapper.class, ImmutableBytesWritable.class,
				ImmutableBytesWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(outputTable, MyReducer.class, job);

		// 必备！！！(缺失会报错：无法加载主类！！！)
		job.waitForCompletion(true);

		WriteRunLog.writeToFiles("instru_org.jar 运行成功！！！");
	}
}
