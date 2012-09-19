package com.chap.memo.memoNodes.model;

import java.io.Serializable;
import java.util.Date;

import com.chap.memo.memoNodes.MemoUtils;
import com.eaio.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class ArcOp implements Serializable, Comparable<ArcOp> {
	private static final long serialVersionUID = 3848738698324348856L;
	protected final OpsType type;
	protected final long uuid10;
	protected final long uuid11;
	protected final long uuid20;
	protected final long uuid21;
	protected final long timestamp;

	ArcOp(OpsType type, UUID[] arc, long timestamp) {
		this.type = type;
		this.uuid10 = arc[0].time;
		this.uuid11 = arc[0].clockSeqAndNode;
		this.uuid20 = arc[1].time;
		this.uuid21 = arc[1].clockSeqAndNode;
		this.timestamp = timestamp;
	}

	ArcOp(OpsType type, UUID[] arc, Date timestamp) {
		this.type = type;
		this.uuid10 = arc[0].time;
		this.uuid11 = arc[0].clockSeqAndNode;
		this.uuid20 = arc[1].time;
		this.uuid21 = arc[1].clockSeqAndNode;
		this.timestamp = timestamp.getTime();
	}

	public ArcOp(OpsType type, UUID parent, UUID child, long timestamp) {
		this.type = type;
		this.uuid10 = parent.time;
		this.uuid11 = parent.clockSeqAndNode;
		this.uuid20 = child.time;
		this.uuid21 = child.clockSeqAndNode;
		this.timestamp = timestamp;
	}

	ArcOp(OpsType type, UUID parent, UUID child, Date timestamp) {
		this.type = type;
		this.uuid10 = parent.time;
		this.uuid11 = parent.clockSeqAndNode;
		this.uuid20 = child.time;
		this.uuid21 = child.clockSeqAndNode;
		this.timestamp = timestamp.getTime();
	}

	public OpsType getType() {
		return type;
	}

	public UUID get(int type) {
		switch (type){
		case 0: 
			return new UUID(uuid10,uuid11);
		case 1:
			return new UUID(uuid20,uuid21);
		default:
			System.out.println("Error: unknown ArcOp type given:"+type);
		}
		return null;
	}

	@JsonIgnore
	public UUID getParent() {
		return new UUID(uuid10,uuid11);
	}

	@JsonIgnore
	public UUID getChild() {
		return new UUID(uuid20,uuid21);
	}
	@JsonIgnore
	public long getParentTime() {
		return MemoUtils.gettime(uuid10);
	}

	@JsonIgnore
	public long getChildTime() {
		return MemoUtils.gettime(uuid20);
	}

	@JsonIgnore
	public Date getTimestamp() {
		return new Date(timestamp);
	}

	public String getParentString(){
		return getParent().toString();
	}
	public String getChildString(){
		return getChild().toString();
	}

	public long getTimestamp_long() {
		return timestamp;
	}

	@Override
	public int compareTo(ArcOp o) {
		return this.timestamp==o.timestamp?0:(this.timestamp>o.timestamp?1:-1);
	}

	@Override
	public String toString() {
		return "[" + this.type + "] " + getParent() + ":" + getChild() + " @"
				+ this.timestamp;
	}
}
