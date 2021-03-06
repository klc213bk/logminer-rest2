package com.transglobe.streamingetl.logminer.rest2.service;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transglobe.streamingetl.logminer.rest2.bean.ApplyLogminerSync;

@Service
public class LogminerService {
	static final Logger logger = LoggerFactory.getLogger(LogminerService.class);

	@Value("${connector.name.orig}")
	private String connectorNameOrig;
	
	@Value("${connect.standlone.script}")
	private String connectStandloneScript;
	
	@Value("${connect.standlone.prop}")
	private String connectStandloneProp;
			
	@Value("${connect.logminer.config}")
	private String connectLogminerConfig;
	
	@Value("${connect.rest.port}")
	private String connectRestPortStr;
	
	@Value("${connect.rest.url}")
	private String connectRestUrl;
	
//	@Value("${connector.name}")
//	private String connectorName;
	
	
//	@Value("${connector.start.default.script}")
//	private String connectorStartDefaultScript;
	
	private Process connectorStartProcess;

	private ExecutorService connectorStartExecutor;
	
	private Map<String,String> origConfigMap;
	
	public void startInitialLogminer() throws Exception {
		logger.info(">>>>>>>>>>>> logminerService.startInitialLogminer starting");
		try {
			if (connectorStartProcess == null || !connectorStartProcess.isAlive()) {
				logger.info(">>>>>>>>>>>> connectorStartProcess.isAlive={} ", (connectorStartProcess == null)? null : connectorStartProcess.isAlive());
			
				ProcessBuilder builder = new ProcessBuilder();
				String script = connectStandloneScript;
				
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
				int i = 0;
				while (true) {
					i++;
					try {
						origConfigMap = getConnectorConfig(connectorNameOrig);
						break;
					} catch (Exception e) {
						logger.warn(">>>>>>>>>>>> trying get config map, but failed, try again");
					}
					Thread.sleep(1000);
					logger.info(">>>> Sleep for 1 second");
					
					if (i > 60) {
						throw new Exception(">>>>> startInitialLogminer Failed after 60 seconds!!!");
					}
				}
				Thread.sleep(15000);
				
				origConfigMap = getConnectorConfig(connectorNameOrig);
				
				logger.info(">>>>>>>>>>>> LogminerService.startInitialLogminer End");
			} else {
				logger.warn(" >>> connectorStartProcess is currently Running.");
			}
		} catch (IOException e) {
			logger.error(">>> Error!!!, startLogminer, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void stopInitialLogminer() throws Exception {
		logger.info(">>>>>>>>>>>> LogminerService.stopInitialLogminer starting...");
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

				logger.info(">>>>>>>>>>>> LogminerService.stopInitialLogminer End");
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
			logger.error(">>> Error!!!, stopInitialLogminer, msg={}, stacktrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
			throw e;
		} 
	}
	public void createInitialConnector(String connectorName) throws Exception {
		
		origConfigMap.put("name", connectorName);
		ObjectMapper mapper = new ObjectMapper();
		String jsonConfigStr = mapper.writeValueAsString(origConfigMap);
		
		String jsonStr = String.format("{\"name\":\"%s\", \"config\":%s}", connectorName, jsonConfigStr);
		
		logger.info(">>>>>>> json={}", jsonStr);
		
		String url = connectRestUrl + "/connectors";
		String response = restPostService(url, jsonStr);
		logger.info(">>>>>>> response={}", response);
		
	}
	public void deleteConnector(String connectorName) throws Exception {
		
		String url = connectRestUrl + "/connectors/" + connectorName;
		
		String response = restService(url, "DELETE");
		
		logger.info(">>>>>>> response={}", response);
		
	}
	public Boolean applyLogminerSync(ApplyLogminerSync applySync) throws Exception {
		logger.info(">>> ApplyLogminerSync={}", ToStringBuilder.reflectionToString(applySync));
		String connectorName = (applySync.getConnectorName() == null)? connectorNameOrig : applySync.getConnectorName();
		Map<String,String>  configmap = getConnectorConfig(connectorName);
		logger.info(">>> original configmap={}", configmap);

		logger.info(">>> updatedConnectorConfigMap");

		String[] tableArr = applySync.getTableListStr().split(",");
		List<String> tableList = Arrays.asList(tableArr);
		Set<String> tableSet = new HashSet<>(tableList);

		updatedConnectorConfigMap(configmap, applySync.getResetOffset(), applySync.getStartScn(), applySync.getApplyOrDrop(), tableSet);

		logger.info(">>> updated configmap={}", configmap);

		logger.info(">>>> add sync table to config's whitelist");

		logger.info(">>>> updateConnector ...");
		Boolean result = updateConnector(connectorName, configmap);
		logger.info(">>>> updateConnector result={}", result);


		return result;
	}
	public boolean updateConnector(String connectorName, Map<String, String> configmap) throws Exception {
		logger.info(">>>>>>>>>>>> updateConnector");

		HttpURLConnection httpConn = null;
		DataOutputStream dataOutStream = null;
		try {

			ObjectMapper objectMapper = new ObjectMapper();
			String configStr = objectMapper.writeValueAsString(configmap);


			String urlStr = connectRestUrl+"/connectors/" + connectorName + "/config";

			logger.info(">>>>> connector urlStr={},reConfigStr={}", urlStr, configStr);

			URL url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("PUT"); 
			httpConn.setDoInput(true);
			httpConn.setDoOutput(true);
			httpConn.setRequestProperty("Content-Type", "application/json");
			httpConn.setRequestProperty("Accept", "application/json");

			dataOutStream = new DataOutputStream(httpConn.getOutputStream());
			dataOutStream.writeBytes(configStr);

			dataOutStream.flush();

			int responseCode = httpConn.getResponseCode();
			logger.info(">>>>> updateConnector responseCode={}",responseCode);

			String readLine = null;

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "UTF-8"));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();
			logger.info(">>>>> updateConnector response={}",response.toString());

			if (200 == responseCode || 201 == responseCode) {
				return true;
			} else {
				return false;
			}

		}  finally {
			if (dataOutStream != null) {
				try {
					dataOutStream.flush();
					dataOutStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (httpConn != null )httpConn.disconnect();

		}
	}
	@SuppressWarnings("unchecked")
	private Map<String,String> getConnectorConfig(String connectorName) throws Exception {


		Map<String,String> configmap = new HashMap<>();
		String urlStr = String.format(connectRestUrl+"/connectors/%s/config", connectorName);
		logger.info(">>>>>>>>>>>> urlStr={} ", urlStr);
		HttpURLConnection httpCon = null;
		try {
			URL url = new URL(urlStr);
			httpCon = (HttpURLConnection)url.openConnection();
			httpCon.setRequestMethod("GET");
			int responseCode = httpCon.getResponseCode();
			String readLine = null;
			//			if (httpCon.HTTP_OK == responseCode) {
			BufferedReader in = new BufferedReader(new InputStreamReader(httpCon.getInputStream()));
			StringBuffer response = new StringBuffer();
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			logger.info(">>>>> CONNECT REST responseCode={},response={}", responseCode, response.toString());

			configmap = (Map<String,String>)(new ObjectMapper().readValue(response.toString(), HashMap.class));

		} finally {
			if (httpCon != null ) httpCon.disconnect();
		}
		return configmap;

	}
	private void updatedConnectorConfigMap(Map<String,String> configmap, Boolean resetOffset, String startScn, int applyOrDrop, Set<String> tableSet) throws Exception {

		logger.info(">>>> configmap={}", configmap);

		if (Boolean.TRUE.equals(resetOffset)) {
			configmap.put("reset.offset", "true");
		} else if (Boolean.FALSE.equals(resetOffset)) {
			configmap.put("reset.offset", "false");
			if (StringUtils.isBlank(startScn)) {
				configmap.put("start.scn", "");
			} else {
				configmap.put("start.scn", startScn);
			}
		}

		if (applyOrDrop == 1) {
			Set<String> newSynTabSet = new HashSet<>();
			String[] origTableArr = configmap.get("table.whitelist").split(",");
			
			for (String tableName : tableSet) {
				boolean match = false;
				for (String tab : origTableArr) {
					if (StringUtils.equalsIgnoreCase(tableName, tab)) {
						match = true;
						break;
					}
				}
				if (!match) {
					newSynTabSet.add(tableName);
				}
			}
			
			String newsyncTables = String.join(",", newSynTabSet);	
			logger.info(">>>> add sync table:{}", newsyncTables);
			
			// "reset.offset", "table.whitelist"
			String newtableWhitelist = "";
			newtableWhitelist = configmap.get("table.whitelist") + "," + newsyncTables;
			newtableWhitelist = StringUtils.strip(newtableWhitelist, ",");
			configmap.put("table.whitelist", newtableWhitelist);


		} else if (applyOrDrop == -1) {
			logger.info(">>>> remove sync tableSet:{}", String.join(",", tableSet));

			String[] tableArr = configmap.get("table.whitelist").split(",");
			List<String> tableList = Arrays.asList(tableArr);
			logger.info(">>>> existing sync tableList:{}", String.join(",", tableList));

			String newtableWhitelist = tableList.stream().filter(s -> !tableSet.contains(s)).collect(Collectors.joining(","));
			newtableWhitelist = StringUtils.strip(newtableWhitelist, ",");
			logger.info(">>>> new newtableWhitelist={}", newtableWhitelist);

			configmap.put("table.whitelist", newtableWhitelist);


		} 

		logger.info(">>>> new configmap={}", configmap);
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
	private String restService(String urlStr, String requestMethod) throws Exception {

		HttpURLConnection httpConn = null;
		URL url = null;
		try {
			url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod(requestMethod);
			int responseCode = httpConn.getResponseCode();
			//			LOG.info(">>>>>  responseCode={}",responseCode);

			BufferedReader in = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
			StringBuffer response = new StringBuffer();
			String readLine = null;
			while ((readLine = in.readLine()) != null) {
				response.append(readLine);
			}
			in.close();

			return response.toString();
		} finally {
			if (httpConn != null ) httpConn.disconnect();
		}
	}
	public static String restPostService(String urlStr, String jsonStr) throws Exception {

		HttpURLConnection httpConn = null;
		URL url = null;
		OutputStream os = null;
		BufferedReader in = null;
		try {
			byte[] input = jsonStr.getBytes("utf-8");
			
			url = new URL(urlStr);
			httpConn = (HttpURLConnection)url.openConnection();
			httpConn.setRequestMethod("POST");
			httpConn.setRequestProperty("Charset", "UTF-8");
			httpConn.setRequestProperty("Content-Type", "application/json" );
			httpConn.setRequestProperty("Accept", "application/json" );
			httpConn.setRequestProperty("Content-Length", String.valueOf(input.length));
			httpConn.setDoOutput(true);

			os = httpConn.getOutputStream();
			
			os.write(input);
			os.flush();
			os.close();

			//			httpConn.setRequestMethod(requestMethod);
			int responseCode = httpConn.getResponseCode();
			logger.info(">>>>>>>>>>>> responseCode={} ", responseCode);
			
			in = new BufferedReader(new InputStreamReader(httpConn.getInputStream(), "utf-8"));
			StringBuffer response = new StringBuffer();
			String readLine = null;
			while ((readLine = in.readLine()) != null) {
				response.append(readLine.trim());
			}
			in.close();

			return response.toString();
		} finally {
			if (os != null) os.close();
			if (in != null) in.close();
			if (httpConn != null ) httpConn.disconnect();
		}
	}
}
