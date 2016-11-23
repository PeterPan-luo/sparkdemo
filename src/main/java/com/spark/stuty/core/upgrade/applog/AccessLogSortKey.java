package com.spark.stuty.core.upgrade.applog;

import java.io.Serializable;

import com.google.common.collect.ComparisonChain;

import scala.math.Ordered;

/**
 * 日志的二次排序key
 * @author user
 *
 */
public class AccessLogSortKey implements Ordered<AccessLogSortKey>, Serializable{

	
	private static final long serialVersionUID = 1L;
	
	private long upTraffic;
	private long downTraffic;
	private long timestamp;
	
	public AccessLogSortKey() {
	}

	public AccessLogSortKey(long upTraffic, long downTraffic, long timestamp) {
		this.upTraffic = upTraffic;
		this.downTraffic = downTraffic;
		this.timestamp = timestamp;
	}

	@Override
	public boolean $greater(AccessLogSortKey other) {
		if(this.upTraffic > other.upTraffic)
			return true;
		else if (this.upTraffic == other.upTraffic &&
				this.downTraffic > other.downTraffic) {
			return true;
		}else if (this.upTraffic == other.upTraffic &&
				this.downTraffic == other.downTraffic &&
				this.timestamp > other.timestamp){
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(AccessLogSortKey other) {
		if ($greater(other)) {
			return true;
		}else if (this.upTraffic == other.upTraffic &&
				this.downTraffic == other.downTraffic &&
				this.timestamp == other.timestamp) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(AccessLogSortKey other) {
		if(this.upTraffic < other.upTraffic)
			return true;
		else if (this.upTraffic == other.upTraffic &&
				this.downTraffic < other.downTraffic) {
			return true;
		}else if (this.upTraffic == other.upTraffic &&
				this.downTraffic == other.downTraffic &&
				this.timestamp < other.timestamp){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(AccessLogSortKey other) {
		if ($less(other)) {
			return true;
		}else if (this.upTraffic == other.upTraffic &&
				this.downTraffic == other.downTraffic &&
				this.timestamp == other.timestamp) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(AccessLogSortKey other) {
		return ComparisonChain.start().compare(this.upTraffic, other.upTraffic)
										.compare(this.downTraffic, other.downTraffic)
										.compare(this.timestamp, other.timestamp)
										.result();
	}

	@Override
	public int compareTo(AccessLogSortKey other) {
		return ComparisonChain.start().compare(this.upTraffic, other.upTraffic)
				.compare(this.downTraffic, other.downTraffic)
				.compare(this.timestamp, other.timestamp)
				.result();
	}

	public long getUpTraffic() {
		return upTraffic;
	}

	public void setUpTraffic(long upTraffic) {
		this.upTraffic = upTraffic;
	}

	public long getDownTraffic() {
		return downTraffic;
	}

	public void setDownTraffic(long downTraffic) {
		this.downTraffic = downTraffic;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (downTraffic ^ (downTraffic >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		result = prime * result + (int) (upTraffic ^ (upTraffic >>> 32));
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
		AccessLogSortKey other = (AccessLogSortKey) obj;
		if (downTraffic != other.downTraffic)
			return false;
		if (timestamp != other.timestamp)
			return false;
		if (upTraffic != other.upTraffic)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "AccessLogSortKey [upTraffic=" + upTraffic + ", downTraffic="
				+ downTraffic + ", timestamp=" + timestamp + "]";
	}
	
}
