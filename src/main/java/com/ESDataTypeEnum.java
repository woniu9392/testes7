package com;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum ESDataTypeEnum {

	INT("class java.lang.Integer", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "integer");
        }
    })), 
	
	LONG("class java.lang.Long", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "long");
        }
    })), 
	
	STRING("class java.lang.String", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "keyword");
        }
    })), 
	
	DATE("class java.util.Date", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "date");
        }
    })), 
	
	BOOLEAN("class java.lang.Boolean", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "boolean");
        }
    })), 
	
	BIGDECIMAL("class java.math.BigDecimal", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "double");
        }
    })),
	
	FLOAT("class java.lang.Float", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "float");
        }
    })),
	
	DOUBLE("class java.lang.Double", Collections.unmodifiableMap(new HashMap<String, String>() {
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
            put("type", "double");
        }
    }));
	
	private ESDataTypeEnum(String type, Map<String, String> map) {
		this.type = type;
		this.map = map;
	}
	
	private String type;
	
	private Map<String, String> map;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Map<String, String> getMap() {
		return map;
	}

	public void setMap(Map<String, String> map) {
		this.map = map;
	}
	
	public static Map<String, String> getMapByType(String type){
		switch(type) {
			case "class java.lang.Integer":
				return INT.map;
			case "class java.lang.Long":
				return LONG.map;
			case "class java.lang.String":
				return STRING.map;
			case "class java.util.Date":
				return DATE.map;
			case "class java.lang.Boolean":
				return BOOLEAN.map;
			case "class java.math.BigDecimal":
				return BIGDECIMAL.map;
			case "class java.lang.Float":
				return FLOAT.map;
			case "class java.lang.Double":
				return DOUBLE.map;
			default:
				return STRING.map;
		}
	}
}
