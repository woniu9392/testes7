package com;

/**
 * 排序类型枚举类
 * @author Ryan.Lv
 *
 */
public enum SortTypeEnum {

	/** 排序类型：升序 **/
	SORT_ASC(1, "升序"),
	
	/** 排序类型：降序 **/
	SORT_DESC(2, "降序");

	private int type;
	
	private String desc;
	
	private SortTypeEnum(int type, String desc) {
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
