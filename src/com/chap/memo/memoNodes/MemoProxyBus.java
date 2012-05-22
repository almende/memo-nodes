package com.chap.memo.memoNodes;

import java.util.ArrayList;
import java.util.HashMap;

import org.msgpack.MessagePack;

import com.eaio.uuid.UUID;

public class MemoProxyBus {
	private final static MemoProxyBus bus = new MemoProxyBus();
	HashMap<UUID,MemoNode> proxyNodes = new HashMap<UUID,MemoNode>();
	private static MessagePack msgpack; 

	private MemoProxyBus(){
		msgpack = new MessagePack();
		msgpack.register(UUID.class);
	}
	static MemoProxyBus getBus() {
		return bus;
	}

	public MessagePack getMsgPack(){
		return msgpack;
	}
	public boolean isProxy(UUID id){
		return proxyNodes.containsKey(id);
	}
	
	MemoNode link(UUID id,String url){
		MemoNode result = new MemoNode(id);
		result.isProxy = true;
		proxyNodes.put(id, result);
		return result;
	}
	
	//Node API
	MemoNode find(UUID id){
		MemoNode result = MemoReadBus.getBus().find(id);
		proxyNodes.put(id, result);
		return result;
	}
	MemoNode find(UUID uuid, long timestamp) {
		return MemoReadBus.getBus().find(uuid,timestamp);
	}
	NodeValue getValue(UUID uuid) {
		return MemoReadBus.getBus().getValue(uuid);
	}
	NodeValue getValue(UUID uuid, long timestamp) {
		return MemoReadBus.getBus().getValue(uuid,timestamp);
	}
	ArrayList<ArcOp> getOps(UUID uuid, int type) {
		return MemoReadBus.getBus().getOps(uuid, type);
	}
	ArrayList<ArcOp> getOps(UUID uuid, int type, long timestamp) {
		return MemoReadBus.getBus().getOps(uuid, type, timestamp);
	}
	NodeValue store(UUID id, byte[] value) {
		return MemoWriteBus.getBus().store(id, value);
	}
	void store(ArcOp op) {
		MemoWriteBus.getBus().store(op);
	}
}
