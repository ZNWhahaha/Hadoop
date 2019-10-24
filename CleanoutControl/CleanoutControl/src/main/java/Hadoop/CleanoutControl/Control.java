package Hadoop.CleanoutControl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

//总控制类
public class Control implements Runnable {
	// 读取的配置文件
	Properties props = new Properties();
	// 程序运行状态
	// 0：未运行 1：运行中 2：运行完成 3：运行错误
	int status[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
	String[] shell = { "java -jar jar_paper/update_profile_id.jar", "java -jar jar_paper/create_hbase_table.jar",
			"hadoop jar jar_paper/author_merger_1.jar", "java -jar jar_paper/create_hbase_table.jar",
			"hadoop jar jar_paper/author_merger_2.jar", "hadoop jar jar_paper/update_paper.jar",
			"hadoop jar jar_paper/paper_person.jar", "java -jar jar_paper/update_personID.jar",
			"hadoop jar jar_paper/person_org_2.jar", "java -jar jar_paper/update_profile_id.jar",
			"hadoop jar jar_paper/co_author.jar" };
	// 接收Kafka消息
	KafkaConsumer<String, String> consumer;
	ExecutorService executor = Executors.newFixedThreadPool(1);
	FutureTask<String> futureTask;

	Control() {
		// 创建Xml
		if (!new File("profile.xml").exists()) {
			initprofile();
			makePaperShell();
		}
		try {
			// 读取配置信息xml
			System.out.println("读取profile.xml基础配置文件");
			props.loadFromXML(new FileInputStream(System.getProperty("user.dir") + "/" + "profile.xml"));
		} catch (InvalidPropertiesFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Kafka连接配置
//		Properties kprops = new Properties();
//		kprops.put("bootstrap.servers", "master:9092");
//		kprops.put("group.id", "test");
//		kprops.put("enable.auto.commit", "true");
//		kprops.put("auto.commit.interval.ms", "1000");
//		kprops.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//		kprops.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
//		// 创建消费者
//		consumer = new KafkaConsumer<String, String>(kprops);
//		consumer.subscribe(Arrays.asList("test"), new ConsumerRebalanceListener() {
//			public void onPartitionsRevoked(Collection<TopicPartition> collection) {
//			}
//
//			public void onPartitionsAssigned(Collection<TopicPartition> collection) {
//				// 将偏移设置到最开始
//				consumer.seekToBeginning(collection);
//			}
//		});
	}

	// 创建默认配置文件
	public void initprofile() {
		System.out.println("创建基本配置文件profile.xml于程序目录：" + System.getProperty("user.dir"));
		// 运行清洗程序需要的所有参数
		Properties props = new Properties();
		props.setProperty("PaperXmlPath", "profile_id/paper_id.xml");
		props.setProperty("TimeStamp", "2017-11-08-22:19:45#2017-11-09-11:36:16");
		props.setProperty("TempTable1", "Author_11");
		props.setProperty("TempTable2", "Author_22");
		props.setProperty("TempFamily", "basicInf,paper");
		props.setProperty("ProvincePath", "hdfs:///user/hadoop/Province.xml");
		props.setProperty("OrgMapPath", "hdfs:///user/hadoop/OrgMap.xml");
		props.setProperty("paperTable", "PAPER");
		props.setProperty("paperTableFamily", "inf");
		props.setProperty("paperTableIDType", "paperID");
		props.setProperty("paperProfileType", "01");
		props.setProperty("paperProfileID", "0");
		props.setProperty("PersonProfile", "PersonProfile");
		props.setProperty("PersonXmlPath", "profile_id/person_id.xml");
		props.setProperty("PersonTableFamily", "basicInf");
		props.setProperty("PersonTableIDType", "personID");
		props.setProperty("PersonProfileType", "01");
		props.setProperty("PersonProfileID", "0");
		props.setProperty("OrgProfile", "OrgProfile");
		props.setProperty("OrgXmlPath", "profile_id/org_id.xml");
		props.setProperty("OrgTableFamily", "basicInf");
		props.setProperty("OrgTableIDType", "orgID");
		props.setProperty("OrgProfileType", "01");
		props.setProperty("OrgProfileID", "0");
		props.setProperty("Paper2AuthorPath", "hdfs:///user/hadoop/Paper2Author");
		this.props = props;
		// 保存至Xml文件
		try {
			props.storeToXML(new FileOutputStream("profile.xml"), "参数配置");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 创建默认参数文件
		// 第一是paper_id.xml
		makePaper_id();
		makePerson_id();
		makeOrg_id();
	}

	// 根据profile创建paper_id.xml文件
	void makePaper_id() {
		File file = new File(System.getProperty("user.home") + "/" + props.getProperty("PaperXmlPath"));
		if (!file.exists()) {
			try {
				File fileDir = new File(System.getProperty("user.dir") + "/" + "profile_id");
				fileDir.mkdirs();
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			PrintStream ps = new PrintStream(new FileOutputStream(file, false));
			ps.print(props.getProperty("paperTable") + "#" + props.getProperty("paperTableFamily") + "#"
					+ props.getProperty("paperTableIDType") + "#" + props.getProperty("paperProfileType") + "#"
					+ props.getProperty("paperProfileID"));
			ps.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 根据profile创建person_id.xml文件
	void makePerson_id() {
		File file = new File(System.getProperty("user.home") + "/" + props.getProperty("PersonXmlPath"));
		if (!file.exists()) {
			try {
				File fileDir = new File(System.getProperty("user.dir") + "/" + "profile_id");
				fileDir.mkdirs();
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			PrintStream ps = new PrintStream(new FileOutputStream(file, false));
			ps.print(props.getProperty("PersonProfile") + "#" + props.getProperty("PersonTableFamily") + "#"
					+ props.getProperty("PersonTableIDType") + "#" + props.getProperty("PersonProfileType") + "#"
					+ props.getProperty("PersonProfileID"));
			ps.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 根据profile创建org_id.xml文件
	void makeOrg_id() {
		File file = new File(System.getProperty("user.home") + "/" + props.getProperty("OrgXmlPath"));
		if (!file.exists()) {
			try {
				File fileDir = new File(System.getProperty("user.dir") + "/" + "profile_id");
				fileDir.mkdirs();
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			PrintStream ps = new PrintStream(new FileOutputStream(file, false));
			ps.print(props.getProperty("OrgProfile") + "#" + props.getProperty("OrgTableFamily") + "#"
					+ props.getProperty("OrgTableIDType") + "#" + props.getProperty("OrgProfileType") + "#"
					+ props.getProperty("OrgProfileID"));
			ps.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 读取POST参数并更新参数
	void updateprofile(String message) {
		String[] keyvalue = message.split("&");
		for (String str : keyvalue) {
			props.setProperty(str.split("=")[0], str.split("=")[1]);
		}
	}

	// 创建运行脚本文件
	void makePaperShell() {
		// 创建文件
		File file = new File(System.getProperty("user.dir") + "/" + "PaperShell.xml");
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		// 脚本步骤
		int i = 1;
		try {
			PrintStream ps = new PrintStream(new FileOutputStream(file, false));
			// 第一步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("PaperXmlPath") + " "
					+ props.getProperty("TimeStamp"));
			i++;
			// 第二步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("TempTable1") + " "
					+ props.getProperty("TempFamily"));
			i++;
			// 第三步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("paperTable") + " "
					+ props.getProperty("TempTable1") + " " + props.getProperty("ProvincePath") + " "
					+ props.getProperty("TimeStamp"));
			i++;
			// 第四步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("TempTable2") + " "
					+ props.getProperty("TempFamily"));
			i++;
			// 第五步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("TempTable1") + " "
					+ props.getProperty("TempTable2") + " " + props.getProperty("OrgMapPath"));
			i++;
			// 第六步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("TempTable2") + " "
					+ props.getProperty("paperTable") + " " + props.getProperty("TimeStamp"));
			i++;
			// 第七步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("TempTable2") + " "
					+ props.getProperty("PersonProfile"));
			i++;
			// 第八步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("PersonXmlPath"));
			i++;
			// 第九步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("PersonProfile") + " "
					+ props.getProperty("OrgProfile"));
			i++;
			// 第十步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("OrgXmlPath"));
			i++;
			// 第十一步
			ps.println("step_" + i + "==>" + shell[i - 1] + " " + props.getProperty("PersonProfile") + " "
					+ props.getProperty("Paper2AuthorPath"));
			i++;
			ps.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// 读取PaperShell的某一条语句
	String readPaperShell(int a) {
		String str = "";
		String string = "";
		File file = new File(System.getProperty("user.dir") + "/" + "PaperShell.xml");
		if (!file.exists()) {
			System.out.println("找不到PaperShell.xml文件");
			return string;
		}
		InputStreamReader inputReader;
		try {
			inputReader = new InputStreamReader(new FileInputStream(file));
			BufferedReader bf = new BufferedReader(inputReader);
			int i = 0;
			while ((str = bf.readLine()) != null && i <= a) {
				string = str;
				i++;
			}
			bf.close();
			inputReader.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return string;
	}

	// 周期更新程序状态与继续程序运行
	public void run() {
		FutureTask<String> futureTask;
		while (true) {
			// 拉取consumer消息
			ConsumerRecords<String, String> records = consumer.poll(100);
			// 循环每条消息
			for (ConsumerRecord<String, String> record : records)
				// 输出消息内容
				System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			// 休息一段时间防止阻塞
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// 控制程序的运行
			String cmd = readPaperShell(0).split("==>")[1];
			CmdThread thread = new CmdThread(cmd);
			futureTask = new FutureTask<String>(thread);
			executor.execute(futureTask);
		}
	}
}

// 控制程序运行的call线程
class CmdThread implements Callable<String> {
	String cmd;
	Runtime runtime = Runtime.getRuntime();
	Process process;

	// 传参构造函数
	CmdThread(String cmd) {
		this.cmd = cmd;
	}

	// 线程函数
	public String call() {
		String line;
		String answer = new String();
		try {
			process = runtime.exec(cmd);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			while ((line = bufferedReader.readLine()) != null) {
				System.out.println(line);
			}
			if (process.waitFor() == 0) {
				answer = "2";
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return answer;
	}

}