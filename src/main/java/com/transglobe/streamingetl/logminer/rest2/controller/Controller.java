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
	
	@PostMapping(path="/startInitialLogminer", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> startInitialLogminer() throws Exception {
		logger.info(">>>>controller startInitialLogminer is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.startInitialLogminer();

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

		logger.info(">>>>controller startInitialLogminer finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
	@PostMapping(path="/stopInitialLogminer", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> stopInitialLogminer() throws Exception {
		logger.info(">>>>controller stopInitialLogminer is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.stopInitialLogminer();

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

		logger.info(">>>>controller stopInitialLogminer finished ");

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
	@PostMapping(path="/createInitialConnector/{connectorName}", produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<Object> createInitialConnector(@PathVariable("connectorName") String connectorName) throws Exception {
		logger.info(">>>>controller createInitialConnector is called");

		ObjectNode objectNode = mapper.createObjectNode();

		try {
			logminerService.createInitialConnector(connectorName);

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

		logger.info(">>>>controller createInitialConnector finished ");

		return new ResponseEntity<Object>(objectNode, HttpStatus.OK);
	}
}
