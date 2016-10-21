package com.spark.stuty.core;

import java.io.Serializable;

import kafka.utils.threadsafe;


import scala.math.Ordered;
/**
 * 二次排序的key
 * @author user
 *
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{
	

	private static final long serialVersionUID = 1L;

	private int firstKey;
	
	private int secondKey;

	
	public SecondarySortKey(int firstKey, int secondKey) {
		this.firstKey = firstKey;
		this.secondKey = secondKey;
	}

	@Override
	public boolean $greater(SecondarySortKey other) {
		if (this.firstKey > other.getFirstKey()) {
			return true;
		}else if (this.firstKey == other.getFirstKey() 
				&& this.secondKey > other.getSecondKey()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(SecondarySortKey other) {
		if (this.$greater(other)) {
			return true;
		}else if (this.firstKey == other.getFirstKey() &&
				this.secondKey == other.getSecondKey()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(SecondarySortKey other) {
		if (this.firstKey < other.getFirstKey()) {
			return true;
		}else if (this.firstKey == other.getFirstKey() && 
				this.secondKey < other.getSecondKey()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(SecondarySortKey other) {
		if (this.$less(other)) {
			return true;
		}else if (this.firstKey == other.firstKey && 
				this.secondKey == other.getSecondKey()) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(SecondarySortKey other) {
		if (this.firstKey != other.getFirstKey()) {
			return this.firstKey - other.getFirstKey();
		}else {
			return this.secondKey - other.getSecondKey();
		}
	}

	@Override
	public int compareTo(SecondarySortKey other) {
		if (this.firstKey != other.getFirstKey()) {
			return this.firstKey - other.getFirstKey();
		}else {
			return this.secondKey - other.getSecondKey();
		}
	}

	public int getFirstKey() {
		return firstKey;
	}

	public void setFirstKey(int firstKey) {
		this.firstKey = firstKey;
	}

	public int getSecondKey() {
		return secondKey;
	}

	public void setSecondKey(int secondKey) {
		this.secondKey = secondKey;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + firstKey;
		result = prime * result + secondKey;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey other = (SecondarySortKey) obj;
		if (firstKey != other.firstKey)
			return false;
		if (secondKey != other.secondKey)
			return false;
		return true;
	}
	
	
	
	
}
