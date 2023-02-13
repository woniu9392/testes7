package com;

/**
 * 查询类型枚举类
 * @author Ryan.Lv
 *
 */
public enum SearchTypeEnum {
	
	/** 精确查询 **/
	TERM_QUERY(0, "精确查询"), 
	
	/** 字符串匹配查询 **/
	PHRASE_QUERY(1, "字符串匹配查询"),
	
	/** 范围查询 **/
	RANGE_QUERY(2, "范围查询"),
	
	/** 模糊查询 **/
	WILD_CARD_QUERY(3, "模糊查询");

	private int type;
	
	private String desc;
	
	private SearchTypeEnum(int type, String desc) {
		this.type = type;
		this.desc = desc;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getDesc() {
		return desc;
	}

	public void setDesc(String desc) {
		this.desc = desc;
	}
}
