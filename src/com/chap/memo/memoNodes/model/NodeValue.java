package com.chap.memo.memoNodes.model;

import java.io.Serializable;
import java.util.Date;

import com.eaio.uuid.UUID;

public class NodeValue implements Serializable,Comparable<NodeValue> {

	private static final long serialVersionUID = 7658842994901044096L;
	private final UUID id;
	private final byte[] value;
	private final long timestamp;

	public NodeValue(UUID id, byte[] value, long timestamp) {
		this.id = id;
		this.value = value;
		this.timestamp = timestamp;
	}

	public NodeValue(UUID id, byte[] value, Date timestamp) {
		this.id = id;
		this.value = value;
		this.timestamp = timestamp.getTime();
	}
	
	public UUID getId() {
		return id;
	}

	public byte[] getValue() {
		return value;
	}

	public Date getTimestamp() {
		return new Date(timestamp);
	}

	public long getTimestamp_long() {
		return timestamp;
	}
	@Override
	public int compareTo(NodeValue o) {
		return timestamp==o.timestamp?0:(timestamp>o.timestamp?1:-1);
	}

}
