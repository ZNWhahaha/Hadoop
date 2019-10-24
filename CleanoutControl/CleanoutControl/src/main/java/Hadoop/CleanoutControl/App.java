package Hadoop.CleanoutControl;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//入口类（通过后台服务器调用运行）
public class App {
	// 后台服务器
	static ServerSocket server;
	// 处理TCP响应的线程池
	static ExecutorService exec;

	public static void main(String args[]) {
		Control control = new Control();
		//基础线程
		Thread t1 = new Thread(control);
		t1.start();
		//设置线程池进行容量为10
		exec = Executors.newFixedThreadPool(10);
		try {
			// 创建Socket服务器
			System.out.println("创建Socket服务器");
			server = new ServerSocket(8081);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		if (server == null)
			System.exit(1);
		Socket client;
		try {
			System.out.println("开始监听请求");
			while (true) {
				client = null;
				client = server.accept();
				if (client != null) {
					// 提交任务
					exec.submit(new ServerThread(client, control));

				}
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}
