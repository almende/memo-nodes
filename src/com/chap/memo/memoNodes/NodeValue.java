package com.chap.memo.memoNodes;

import java.io.Serializable;
import java.util.Date;

import com.eaio.uuid.UUID;

public class NodeValue implements Serializable {
	private static final long serialVersionUID = 7658842994901044096L;
	private final UUID id;
	private final byte[] value;
	private final long timestamp;
	
	protected NodeValue(UUID id, byte[] value, long timestamp){
		this.id = id;
		this.value = value;
		this.timestamp = timestamp;
	}
	protected NodeValue(UUID id, byte[] value, Date timestamp){
		this.id = id;
		this.value = value;
		this.timestamp = timestamp.getTime();
	}
	protected UUID getId() {
		return id;
	}

	protected byte[] getValue() {
		return value;
	}

	protected Date getTimestamp() {
		return new Date(timestamp);
	}
	protected long getTimestamp_long(){
		return timestamp;
	}
	
}
