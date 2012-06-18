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

	public NodeValue(byte[] msg) throws IOException{
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayInputStream in = new ByteArrayInputStream(msg);
	    Unpacker unpacker = msgpack.createUnpacker(in);
    	this.id = unpacker.read(UUID.class);
		this.value = unpacker.read(byte[].class);
		this.timestamp = unpacker.read(long.class);
    }
	
	public byte[] toMsg() throws IOException{
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Packer packer = msgpack.createPacker(out);
		packer.write(this.id);
		packer.write(this.value);
		packer.write(this.timestamp);
		return out.toByteArray();
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
