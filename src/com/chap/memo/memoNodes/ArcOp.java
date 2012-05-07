package com.chap.memo.memoNodes;

import java.io.Serializable;
import java.util.Date;

import com.eaio.uuid.UUID;

public class ArcOp implements Serializable,Comparable<ArcOp> {
	private static final long serialVersionUID = 3848738698324348856L;
	private final Ops type; 
	private final UUID[] arc;
	private final long timestamp;
	
	protected ArcOp(Ops type,UUID[] arc,long timestamp){
		this.type=type;
		this.arc = arc;
		this.timestamp=timestamp;
	}
	protected ArcOp(Ops type,UUID[] arc,Date timestamp){
		this.type=type;
		this.arc = arc;
		this.timestamp=timestamp.getTime();
	}
	protected ArcOp(Ops type,UUID parent,UUID child,long timestamp){
		this.type=type;
		this.arc = new UUID[2];
		this.arc[0]=parent;
		this.arc[1]=child;
		this.timestamp=timestamp;
	}
	protected ArcOp(Ops type,UUID parent,UUID child,Date timestamp){
		this.type=type;
		this.arc = new UUID[2];
		this.arc[0]=parent;
		this.arc[1]=child;
		this.timestamp=timestamp.getTime();
	}
	protected Ops getType() {
		return type;
	}
	protected UUID get(int type){
		return arc[type];
	}
	protected UUID getParent() {
		return arc[0];
	}
	protected UUID getChild() {
		return arc[1];
	}
	protected Date getTimestamp() {
		return new Date(timestamp);
	}
	protected long getTimestamp_long() {
		return timestamp;
	}
	
	@Override
	public int compareTo(ArcOp o) {
		return (int) ((this.timestamp - o.timestamp)%1);
	}
	@Override
	public String toString(){
		return "["+this.type+"] "+this.arc[0]+":"+this.arc[1]+" @"+this.timestamp;
	}
}
