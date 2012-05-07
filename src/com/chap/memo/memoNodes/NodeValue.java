package com.chap.memo.memoNodes;

import java.io.Serializable;
import java.util.Date;

import com.eaio.uuid.UUID;

public class NodeValue implements Serializable {
	private static final long serialVersionUID = 7658842994901044096L;
	private final UUID id;
	private final byte[] value;
	private final long timestamp;
	
	 NodeValue(UUID id, byte[] value, long timestamp){
		this.id = id;
		this.value = value;
		this.timestamp = timestamp;
	}
	 NodeValue(UUID id, byte[] value, Date timestamp){
		this.id = id;
		this.value = value;
		this.timestamp = timestamp.getTime();
	}
	 UUID getId() {
		return id;
	}

	 byte[] getValue() {
		return value;
	}

	 Date getTimestamp() {
		return new Date(timestamp);
	}
	 long getTimestamp_long(){
		return timestamp;
	}
	
}
