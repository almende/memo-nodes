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
	protected final UUID[] arc;
	protected final long timestamp;

	ArcOp(OpsType type, UUID[] arc, long timestamp) {
		this.type = type;
		this.arc = arc;
		this.timestamp = timestamp;
	}

	ArcOp(OpsType type, UUID[] arc, Date timestamp) {
		this.type = type;
		this.arc = arc;
		this.timestamp = timestamp.getTime();
	}

	ArcOp(OpsType type, UUID parent, UUID child, long timestamp) {
		this.type = type;
		this.arc = new UUID[2];
		this.arc[0] = parent;
		this.arc[1] = child;
		this.timestamp = timestamp;
	}

	ArcOp(OpsType type, UUID parent, UUID child, Date timestamp) {
		this.type = type;
		this.arc = new UUID[2];
		this.arc[0] = parent;
		this.arc[1] = child;
		this.timestamp = timestamp.getTime();
	}
	
	public ArcOp(byte[] msg) throws IOException{
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayInputStream in = new ByteArrayInputStream(msg);
	    Unpacker unpacker = msgpack.createUnpacker(in);
    	this.type = unpacker.read(OpsType.class);
		this.arc = unpacker.read(UUID[].class);
		this.timestamp = unpacker.read(long.class);
    }
	
	public byte[] toMsg() throws IOException{
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Packer packer = msgpack.createPacker(out);
		packer.write(this.type);//Will this work correctly for enums?
		packer.write(this.arc);
		packer.write(this.timestamp);
		return out.toByteArray();
	}
	public OpsType getType() {
		return type;
	}

	public UUID get(int type) {
		return arc[type];
	}

	public UUID getParent() {
		return arc[0];
	}

	public UUID getChild() {
		return arc[1];
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
		return "[" + this.type + "] " + this.arc[0] + ":" + this.arc[1] + " @"
				+ this.timestamp;
	}
}
