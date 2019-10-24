package com.shuting.Project_Achieve_Person2;

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

public class AchievePerson2 {

	public static class MyMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String filePath = conf.get("filePath");
			ConfigInf.initOrgMap(filePath);
		}

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
			// 信息来自成果奖励表
			else {
				String achieveID2 = rowkey;
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
						achieveName = achieveName.replace("（", "(");
						achieveName = achieveName.replace("）", ")");

						achieveName = achieveName.replace("“", "\"");
						achieveName = achieveName.replace("”", "\"");

						achieveName = achieveName.replace("，", ",");
					} else if ("achieveID".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						achieveID = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					} else if ("main_people".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						personStr = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
						personStr = personStr.replaceAll("（", "(");
						personStr = personStr.replaceAll("）", ")");
					} else if ("main_unit".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						orgStr = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
						orgStr = orgStr.replaceAll("（", "(");
						orgStr = orgStr.replaceAll("）", ")");
					} else if ("province_or_country".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						jibie = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					} else if ("reward_level".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						dengji = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					} else if ("reward_type".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						leixing = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					} else if ("year".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
						year = Bytes.toString(CellUtil.cloneValue(cell)).replaceAll("\\s*", "");
					}
				}
				if (personStr.isEmpty()) {
					return;
				}
				if (jibie.isEmpty()) {
					jibie = "暂无填写";
				}
				if (!orgStr.isEmpty()) {

					ArrayList<String> orgSet = new ArrayList<String>();
					ArrayList<String> personSet = new ArrayList<String>();
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
							} else {
								int left = tempStr.indexOf("(");
								org = tempStr.substring(0, left);
							}
						} else {
							org = tempStr;
						}
						Organization orgClean = new Organization(org);
						org = orgClean.handleOrg();
						orgSet.add(org);
					}
					String orgNameAll = "";
					for (int i = 0; i < orgSet.size(); i++) {
						orgNameAll += (orgNameAll.length() > 0 ? ";" : "") + orgSet.get(i);
					}

					// 形式：冉新权、杨华、李安琪、付金华、何顺利、李忠兴、窦伟坦、赵继勇
					String[] splitStr2 = personStr.split("、");
					for (int i = 0; i < splitStr2.length; i++) {
						String tempStr = splitStr2[i];
						String name = "";
						if (tempStr.contains("(")) {
							int index = tempStr.indexOf("(");
							name = tempStr.substring(0, index);
						} else {
							name = tempStr;
						}
						personSet.add(name);
					}

					String valueStr = orgNameAll + "<=>" + achieveID + "=>" + achieveID2 + "->" + achieveName + "->"
							+ jibie + ";" + dengji + ";" + leixing + ";" + year;
					ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
					for (int i = 0; i < personSet.size(); i++) {
						ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(personSet.get(i)));
						context.write(key, value);
					}
				} else {
					// 卓宁生(陕西飞机工业（集团）有限公司)
					String[] splitStr2 = personStr.split("、");
					for (int i = 0; i < splitStr2.length; i++) {
						String tempStr = splitStr2[i];
						String name = "";
						String org = "";
						if (tempStr.contains("(")) {
							int left = tempStr.indexOf("(");
							name = tempStr.substring(0, left);
							if (tempStr.contains(")")) {
								int right = tempStr.lastIndexOf(")");
								org = tempStr.substring(left + 1, right);
							} else {
								org = tempStr.substring(left + 1);
							}

							if (org.length() > 3) {
								if (org.contains("(") && org.contains(")")) {
									int left2 = tempStr.indexOf("(");
									int right2 = tempStr.indexOf(")");
									String subStr = tempStr.substring(left2, right2 + 1);
									org = org.replace(subStr, "");
								}
								Organization orgClean = new Organization(org);
								org = orgClean.handleOrg();
								String valueStr = org + "<=>" + achieveID + "=>" + achieveID2 + "->" + achieveName
										+ "->" + jibie + ";" + dengji + ";" + leixing + ";" + year;
								ImmutableBytesWritable value = new ImmutableBytesWritable(Bytes.toBytes(valueStr));
								ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytes(name));
								context.write(key, value);
							}
						}
					}
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
			// 存储成果完成人信息
			ArrayList<String> achieveOrgSet = new ArrayList<String>();
			ArrayList<String> achieveInfSet = new ArrayList<String>();

			for (ImmutableBytesWritable val : values) {

				String valueStr = Bytes.toString(val.get());
				// 信息来自成果库
				if (valueStr.contains("<=>")) {
					String[] splitStr = valueStr.split("<=>");
					if (!achieveOrgSet.contains(splitStr[0])) {
						achieveOrgSet.add(splitStr[0]);
						achieveInfSet.add(splitStr[1]);
					} else {
						int index = achieveOrgSet.indexOf(splitStr[0]);
						String achieveStr = achieveInfSet.get(index);
						achieveStr = achieveStr + "<==>" + splitStr[1];
						achieveInfSet.set(index, achieveStr);
					}
				} else {
					personOrgSet.add(valueStr);
				}
			}

			if (achieveOrgSet.isEmpty()) {
				return;
			}

			for (int i = 0; i < achieveOrgSet.size(); i++) {

				String[] personOrgs = achieveOrgSet.get(i).split(";");
				Boolean isPut = false;
				for (int j = 0; j < personOrgs.length; j++) {
					String personOrg = personOrgs[j];
					Boolean isExist = false;
					int personIndex = 0;
					for (int k = 0; k < personOrgSet.size(); k++) {
						if (personOrgSet.get(k).contains(personOrg)) {
							isExist = true;
							personIndex = k;
							break;
						}
					}
					if (isExist) {
						isPut = true;
						String keyStr = nameCH + "->" + personOrgSet.get(personIndex);
						Put put = new Put(keyStr.getBytes());
						String[] achieveSet = achieveInfSet.get(i).split("<==>");
						for (int k = 0; k < achieveSet.length; k++) {

							String achieveID = achieveSet[k].split("=>")[0];
							String cellValue = achieveSet[k].split("=>")[1];
							put.addColumn(Bytes.toBytes("achieve"), Bytes.toBytes(achieveID), Bytes.toBytes(cellValue));
							context.write(null, put);
						}
						break;
					}
				}
				// 如果未找到对应的人
				if (isPut == false) {
					continue;
				}
			}
		}
	}

	// 成果奖励库更新人才库
	public static void main(String[] args) throws Exception {

		WriteRunLog.writeToFiles("正在运行的是: achieveAward_person.jar ......");
		if (args.length != 3) {
			WriteRunLog.writeToFiles("抱歉，您没有输入正确的参数个数（3）");
			System.exit(0);
		}
		String inputTable = args[0];
		String outputTable = args[1];

		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("filePath", args[2]);
		Job job = Job.getInstance(hbaseConf, "achieveAward_person");
		job.setJarByClass(AchievePerson2.class);

		ConfigInf.initOrgMap(args[2]);

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

		WriteRunLog.writeToFiles("achieveAward_person.jar 运行成功！！！");
	}
}
