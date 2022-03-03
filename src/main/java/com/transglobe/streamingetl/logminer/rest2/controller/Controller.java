package com.transglobe.streamingetl.logminer.rest2.controller;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.transglobe.streamingetl.logminer.rest2.bean.ApplyLogminerSync;
import com.transglobe.streamingetl.logminer.rest2.service.LogminerService;

@RestController
@RequestMapping("/")
public class Controller {
	static final Logger logger = LoggerFactory.getLogger(Controller.class);

	@Autowired
	private LogminerService logminerService;

	@Autowired
	private ObjectMapper mapper;
	
	@PostMapping(path="/startLogminer", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startLogminer() throws Exception {
		logger.info(">>>>controller startLogminer is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.startLogminer();

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		logger.info(">>>>controller startLogminer finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/stopLogminer", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> stopLogminer() throws Exception {
		logger.info(">>>>controller stopLogminer is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.stopLogminer();

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		logger.info(">>>>controller stopLogminer finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/applyLogminerSync", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> applyLogminerSync(@RequestBody ApplyLogminerSync applySync) throws Exception {
		logger.info(">>>>controller applyLogminerSync is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			//			LOG.info(">>>>applySync={}", ToStringBuilder.reflectionToString(applySync));

			Boolean result = logminerService.applyLogminerSync(applySync);

			objectNode.put("returnCode", "0000");
			objectNode.put("status", (result == null)? Boolean.FALSE.toString() : result.toString());
		
			logger.info(">>>>controller applyLogminerSync finished ");

			return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
			
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		
	}
	@PostMapping(path="/createDefaultConnector/{connectorName}/{logminerClient}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> createDefaultConnector(@PathVariable("connectorName") String connectorName, @PathVariable("logminerClient") String logminerClient) throws Exception {
		logger.info(">>>>controller createDefaultConnector is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.createDefaultConnector(connectorName, logminerClient);

			objectNode.put("returnCode", "0000");
		} catch (Exception e) {
			String errMsg = ExceptionUtils.getMessage(e);
			String stackTrace = ExceptionUtils.getStackTrace(e);
			objectNode.put("returnCode", "-9999");
			objectNode.put("errMsg", errMsg);
			objectNode.put("returnCode", stackTrace);
			logger.error(">>> errMsg={}, stacktrace={}",errMsg,stackTrace);
			throw e;
		}

		logger.info(">>>>controller createDefaultConnector finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
