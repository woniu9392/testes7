package com;

import java.util.List;

import lombok.Data;

@Data
public class Page<T> {

	private Class<T> clazz;
	
	private int pageNo;
	
	private int pageSize;
	
	private float totalCount;
	
	private List<T> list;
}
