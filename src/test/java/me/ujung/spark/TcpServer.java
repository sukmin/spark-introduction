package me.ujung.spark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.google.gson.Gson;

/**
 * @author sukmin.kwon
 * @since 2017-02-21
 */
public class TcpServer {

	@Test
	public void startServer() throws Exception {

		int port = 33334;

		try (ServerSocket listenSocket = new ServerSocket(port)) {

			System.out.println("Start server");

			Executor executor = Executors.newFixedThreadPool(20);

			// 클라이언트가 연결될때까지 대기한다.
			Socket clientSocket;
			while ((clientSocket = listenSocket.accept()) != null) {
				System.out.println("connect ip : " + clientSocket.getInetAddress().getHostAddress());
				RequestHandler requestHandler = new RequestHandler(clientSocket);
				executor.execute(requestHandler);
			}
		}

	}

	public static class RequestHandler implements Runnable {

		private static final AtomicLong ATOMIC_LONG = new AtomicLong(1L);

		private static final Gson gson = new Gson();

		Socket clientSocket;
		String clientIp;

		public RequestHandler(Socket clientSocket) {
			this.clientSocket = clientSocket;
			this.clientIp = clientSocket.getInetAddress().getHostAddress();
		}

		@Override
		public void run() {
			Random random = new Random();

			try (OutputStream outputStream = clientSocket.getOutputStream();
				 OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
				 BufferedWriter bufferedWriter = new BufferedWriter(outputStreamWriter);
			) {
				while (true) {
					String name = NAMES[random.nextInt(NAMES.length)];
					int score = random.nextInt(100) + 1;
					TestModel testModel = new TestModel(ATOMIC_LONG.getAndIncrement(), name, score);
					String data = gson.toJson(testModel);
					bufferedWriter.write(data + "\n");
					bufferedWriter.flush();
					Thread.sleep(100);
				}
			} catch (Exception e) {
				e.printStackTrace();
				try {
					clientSocket.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

		}
	}

	private static final String[] NAMES = {
		"덴마", "지로", "고산", "다이크", "공자", "가우스", "주완", "우루사", "엘", "누브레", "하즈", "헤글러", "다니엘",
		"제시카", "티파니", "태연", "서현", "유리", "써니", "효연", "윤아", "수영",
		"나연", "정연", "모모", "사나", "지효", "미나", "다현", "채영", "쯔위",
		"전소미", "김세정", "최유정", "김청하", "김소혜", "주결경", "정채연", "김도연", "강미나", "임나영", "유연정"
	};

}
