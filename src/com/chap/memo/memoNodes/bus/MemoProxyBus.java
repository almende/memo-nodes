package com.chap.memo.memoNodes.bus;

import java.util.ArrayList;
import java.util.HashMap;

import org.msgpack.MessagePack;

import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.NodeValue;
import com.eaio.uuid.UUID;

public class MemoProxyBus {
	private final static MemoProxyBus bus = new MemoProxyBus();
	HashMap<UUID,MemoNode> proxyNodes = new HashMap<UUID,MemoNode>(10000);
	private static MessagePack msgpack; 

	private MemoProxyBus(){
		msgpack = new MessagePack();
		msgpack.register(UUID.class);
	}
	public static MemoProxyBus getBus() {
		return bus;
	}

	public MessagePack getMsgPack(){
		return msgpack;
	}
	public boolean isProxy(UUID id){
		return proxyNodes.containsKey(id);
	}
	
	public MemoNode link(UUID id,String url){
		MemoNode result = new MemoNode(id);
		result.isProxy = true;
		proxyNodes.put(id, result);
		return result;
	}
	
	//Node API
	public MemoNode find(UUID id){
		MemoNode result = MemoReadBus.getBus().find(id);
		proxyNodes.put(id, result);
		return result;
	}
	public MemoNode find(UUID uuid, long timestamp) {
		return MemoReadBus.getBus().find(uuid,timestamp);
	}
	public NodeValue getValue(UUID uuid) {
		return MemoReadBus.getBus().getValue(uuid);
	}
	public NodeValue getValue(UUID uuid, long timestamp) {
		return MemoReadBus.getBus().getValue(uuid,timestamp);
	}
	public ArrayList<ArcOp> getOps(UUID uuid, int type, long since) {
		return MemoReadBus.getBus().getOps(uuid, type, since);
	}
	public ArrayList<ArcOp> getOps(UUID uuid, int type, long timestamp, long since) {
		return MemoReadBus.getBus().getOps(uuid, type, timestamp, since);
	}
	public NodeValue store(UUID id, byte[] value) {
		return MemoWriteBus.getBus().store(id, value);
	}
	public void store(ArcOp op) {
		MemoWriteBus.getBus().store(op);
	}
}
