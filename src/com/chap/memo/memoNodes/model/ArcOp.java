package com.chap.memo.memoNodes.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import com.chap.memo.memoNodes.bus.MemoProxyBus;
import com.eaio.uuid.UUID;

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

	ArcOp(OpsType type, UUID parent, UUID child, long timestamp) {
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
	
	public ArcOp(byte[] msg) throws IOException{
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayInputStream in = new ByteArrayInputStream(msg);
	    Unpacker unpacker = msgpack.createUnpacker(in);
    	this.type = unpacker.read(OpsType.class);
    	this.uuid10 = unpacker.read(long.class);
    	this.uuid11 = unpacker.read(long.class);
    	this.uuid20 = unpacker.read(long.class);
    	this.uuid21 = unpacker.read(long.class);
		this.timestamp = unpacker.read(long.class);
    }
	
	public byte[] toMsg() throws IOException{
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Packer packer = msgpack.createPacker(out);
		packer.write(this.type);//Will this work correctly for enums?
		packer.write(this.uuid10);
		packer.write(this.uuid11);
		packer.write(this.uuid20);
		packer.write(this.uuid21);
		packer.write(this.timestamp);
		return out.toByteArray();
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

	public UUID getParent() {
		return new UUID(uuid10,uuid11);
	}

	public UUID getChild() {
		return new UUID(uuid20,uuid21);
	}

	public Date getTimestamp() {
		return new Date(timestamp);
	}

	public long getTimestamp_long() {
		return timestamp;
	}

	@Override
	public int compareTo(ArcOp o) {
		return (int) ((this.timestamp - o.timestamp) % 1);
	}

	@Override
	public String toString() {
		return "[" + this.type + "] " + getParent() + ":" + getChild() + " @"
				+ this.timestamp;
	}
}
