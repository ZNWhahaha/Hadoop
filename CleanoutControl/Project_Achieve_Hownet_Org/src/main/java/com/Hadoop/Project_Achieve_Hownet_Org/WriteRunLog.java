package com.Hadoop.Project_Achieve_Hownet_Org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileWriter;
import java.io.IOException;

public class WriteRunLog {

	private WriteRunLog() {
		throw new Error("不要实例化我!!!");
	}

	public static void writeToFiles(String dataStr) {

		String logPath = "/home/hadoop/Kafka_Project/logs/log.xml";
		try {
			FileWriter writer = new FileWriter(logPath, true);
			writer.write(dataStr + "\n");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void exceptionLog(String dataStr) {
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path filePath = new Path("hdfs:///user/zhishuting/ExceptionLog.xml");
			FSDataOutputStream outputStream = fs.create(filePath);
			outputStream.writeBytes(dataStr + "\n");
			outputStream.close();
			fs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
