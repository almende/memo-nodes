package com.chap.memo.memoNodes.NewImpl;

import java.io.Serializable;
import java.util.Date;

import com.eaio.uuid.UUID;

public class ArcOp implements Serializable {
	private static final long serialVersionUID = 3848738698324348856L;
	private final Ops type; 
	private final UUID parent;
	private final UUID child;
	private final long timestamp;
	
	public ArcOp(Ops type,UUID parent,UUID child,long timestamp){
		this.type=type;
		this.parent=parent;
		this.child=child;
		this.timestamp=timestamp;
	}
	public ArcOp(Ops type,UUID parent,UUID child,Date timestamp){
		this.type=type;
		this.parent=parent;
		this.child=child;
		this.timestamp=timestamp.getTime();
	}
	public Ops getType() {
		return type;
	}
	public UUID getParent() {
		return parent;
	}
	public UUID getChild() {
		return child;
	}
	public Date getTimestamp() {
		return new Date(timestamp);
	}
	public long getTimestamp_long() {
		return timestamp;
	}
}
