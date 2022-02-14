package com.transglobe.streamingetl.logminer.rest2.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class LogminerService {
	static final Logger logger = LoggerFactory.getLogger(LogminerService.class);

	@Value("${connect.standlone.script}")
	private String connectStandloneScript;
	
	@Value("${connect.standlone.prop}")
	private String connectStandloneProp;
			
	@Value("${connect.logminer.config}")
	private String connectLogminerConfig;
	
	@Value("${connect.rest.port}")
	private String connectRestPortStr;
	
//	@Value("${connector.start.default.script}")
//	private String connectorStartDefaultScript;
	
	private Process connectorStartProcess;

	private ExecutorService connectorStartExecutor;
	
	public void startLogminer() throws Exception {
		logger.info(">>>>>>>>>>>> logminerService.startLogminer starting");
		try {
			if (connectorStartProcess == null || !connectorStartProcess.isAlive()) {
				logger.info(">>>>>>>>>>>> connectorStartProcess.isAlive={} ", (connectorStartProcess == null)? null : connectorStartProcess.isAlive());
			
				ProcessBuilder builder = new ProcessBuilder();
				String script = connectStandloneScript;
				String props = connectStandloneProp + " " + connectLogminerConfig;
				//builder.command("sh", "-c", script);
				builder.command(script, connectStandloneProp, connectLogminerConfig);

				builder.directory(new File("."));
				connectorStartProcess = builder.start();

				connectorStartExecutor = Executors.newSingleThreadExecutor();
				connectorStartExecutor.submit(new Runnable() {

					@Override
					public void run() {
						BufferedReader reader = new BufferedReader(new InputStreamReader(connectorStartProcess.getInputStream()));
						reader.lines().forEach(line -> {
							logger.info(line);
						});
					}

				});
				int connectRestPort = Integer.valueOf(connectRestPortStr);
				while (!checkPortListening(connectRestPort)) {
					Thread.sleep(1000);
					logger.info(">>>> Sleep for 1 second");;
				}
				Thread.sleep(15000);
				logger.info(">>>>>>>>>>>> KafkaService.startKafka End");

				logger.info(">>>>>>>>>>>> LogminerService.startConnector End");
			} else {
				logger.warn(" >>> connectorStartProcess is currently Running.");
			}
		} catch (IOException e) {
			logger.error(">>> Error!!!, startLogminer, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopLogminer() throws Exception {
		logger.info(">>>>>>>>>>>> LogminerService.stopLogminer starting...");
		try {
			if (connectorStartProcess != null && connectorStartProcess.isAlive()) {
				logger.info(">>>>>>>>>>>> connectorStartProcess.isAlive={} ", (connectorStartProcess == null)? null : connectorStartProcess.isAlive());
				//				
				connectorStartProcess.destroy();

				int kafkaServerPort = Integer.valueOf(connectRestPortStr);
				while (checkPortListening(kafkaServerPort)) {
					Thread.sleep(10000);
					logger.info(">>>> Sleep for 10 second");;
				}

				logger.info(">>>>>>>>>>>> LogminerService.stopConnector End");
			} else {
				logger.warn(" >>> connectorStartProcess IS NOT ALIVE.");
			}

			if (!connectorStartExecutor.isTerminated()) {
				if (!connectorStartExecutor.isShutdown()) {
					connectorStartExecutor.shutdown();
				}
				while (!connectorStartExecutor.isShutdown()) {
					Thread.sleep(1000);
					logger.info(">>>> waiting for executor shuttung down.");;
				}

				while (!connectorStartExecutor.isTerminated()) {
					Thread.sleep(1000);
					logger.info(">>>> waiting for executor termainting.");;
				}
			} 
			if (connectorStartExecutor.isTerminated()) {
				logger.info(">>>> connectorStartExecutor is Terminated!!!!!");
			} 

		} catch (IOException e) {
			logger.error(">>> Error!!!, stopLogminer, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	private boolean checkPortListening(int port) throws Exception {
		logger.info(">>>>>>>>>>>> checkPortListening:{} ", port);

		BufferedReader reader = null;
		try {
			ProcessBuilder builder = new ProcessBuilder();
			String script = "netstat -tnlp | grep :" + port;
			builder.command("bash", "-c", script);
			//				builder.command(kafkaTopicsScript + " --list --bootstrap-server " + kafkaBootstrapServer);

			//				builder.command(kafkaTopicsScript, "--list", "--bootstrap-server", kafkaBootstrapServer);

			builder.directory(new File("."));
			Process checkPortProcess = builder.start();

			AtomicBoolean portRunning = new AtomicBoolean(false);


			int exitVal = checkPortProcess.waitFor();
			if (exitVal == 0) {
				reader = new BufferedReader(new InputStreamReader(checkPortProcess.getInputStream()));
				reader.lines().forEach(line -> {
					if (StringUtils.contains(line, "LISTEN")) {
						portRunning.set(true);
						logger.info(">>> Success!!! portRunning.set(true)");
					}
				});
				reader.close();

				logger.info(">>> Success!!! portRunning={}", portRunning.get());
			} else {
				logger.error(">>> Error!!!  exitcode={}", exitVal);


			}
			if (checkPortProcess.isAlive()) {
				checkPortProcess.destroy();
			}

			return portRunning.get();
		} finally {
			if (reader != null) reader.close();
		}

	}
}
