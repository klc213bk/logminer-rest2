package com.transglobe.streamingetl.logminer.rest2.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ApplyLogminerSync {

	@JsonProperty("resetOffset")
	private Boolean resetOffset;
	
	@JsonProperty("startScn")
	private String startScn;
	
	@JsonProperty("applyOrDrop")
	private Integer applyOrDrop; // 1 :appply, -1: drop
	
	@JsonProperty("tableListStr")
	private String tableListStr; // table separate with ','

	public ApplyLogminerSync() {}
	
	public Boolean getResetOffset() {
		return resetOffset;
	}

	public void setResetOffset(Boolean resetOffset) {
		this.resetOffset = resetOffset;
	}

	public String getStartScn() {
		return startScn;
	}

	public void setStartScn(String startScn) {
		this.startScn = startScn;
	}

	public Integer getApplyOrDrop() {
		return applyOrDrop;
	}

	public void setApplyOrDrop(Integer applyOrDrop) {
		this.applyOrDrop = applyOrDrop;
	}

	public String getTableListStr() {
		return tableListStr;
	}

	public void setTableListStr(String tableListStr) {
		this.tableListStr = tableListStr;
	}
	
	
}
