package com;

import java.io.Serializable;

import lombok.Data;

@Data
public class TestEntity implements Serializable {

	private static final long serialVersionUID = 1L;

	private String id;
	
	private Long picreateId;
	
	private String requserName;
}
