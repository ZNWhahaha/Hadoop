package Hadoop.CleanoutControl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URLDecoder;
import java.util.Date;

//处理TCP请求
public class ServerThread implements Runnable {
	private Socket client;
	private Control control;

	ServerThread(Socket client, Control control) {
		this.client = client;
		this.control = control;
	}

	public void run() {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
			// GET /test.jpg /HTTP1.1
			// String line = reader.readLine();
			// String resource = line.substring(line.indexOf('/'), line.lastIndexOf('/') -
			// 5);
			// System.out.println("请求内容为: " + resource);
			// resource = URLDecoder.decode(resource, "UTF-8");
			String line;
			int lenth = 0;
			boolean POST = false;
			line = reader.readLine();
			System.out.println(line);
			if (line.split("/")[0].equals("POST "))
				POST = true;
			String resource = "";
			if (!POST)
				resource = line.substring(line.indexOf('/') + 1, line.lastIndexOf('/') - 5);
			while (line != null) {
				line = reader.readLine();
				System.out.println(line);
				if (line.contains("Content-Length"))
					lenth = Integer.parseInt(line.split(": ")[1]);
				if (line.equals("")) {
					break;
				}
			}
			if (POST) {
				StringBuilder str = new StringBuilder();
				for (int i = 0; i < lenth; i++) {
					str.append((char) reader.read());
				}
				line = str.toString();
				System.out.println(line);
			}
			// HTTP返回报文
			PrintStream out = new PrintStream(client.getOutputStream());
			out.println("HTTP/1.1 200 OK");
			out.println("Content-Type: text/html;charset=UTF-8");
			out.println("Date: " + new Date());
			// 判断逻辑
			String response = "{\"status\":\"200}";
			String backup = "[{\"key\":\"key\",\"value\":\"value\"}]";
			out.println("Content-Length: " + response.length());
			out.println();
			out.print(response);
			// 报文结束
			out.flush();
			out.close();
			reader.close();
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
