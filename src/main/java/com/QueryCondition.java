package com;

import java.util.List;

import lombok.Data;

@Data
public class QueryCondition {
	
	private short queryType;

	private String fieldName;

	private boolean isAndRelation;
	
	private List<String> conditions;
	
	private Object rangeStartCondition;
	
	private Object rangeEndCondition;
}
