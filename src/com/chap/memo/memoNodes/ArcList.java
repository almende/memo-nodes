package com.chap.memo.memoNodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import com.eaio.uuid.UUID;

public class ArcList {
	MemoReadBus readBus = MemoReadBus.getBus();
	MemoWriteBus writeBus = MemoWriteBus.getBus();
	MemoProxyBus proxyBus = MemoProxyBus.getBus();
	private boolean isProxy = false;
	long lastUpdate = 0;

	UUID[] nodes = new UUID[0];
	ArrayList<ArcOp> arcops = null;
	int type; // 0: parent list, 1:child list
	UUID nodeId;

	private long timestamp;

	ArcList(UUID nodeId, int type, boolean isProxy) {
		this.type = type;
		this.nodeId = nodeId;
		this.isProxy = isProxy;
	}

	ArcList(UUID nodeId,int type,byte[] msg) throws IOException{
		this.type = type;
		this.nodeId = nodeId;
		this.isProxy = true;
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayInputStream in = new ByteArrayInputStream(msg);
	    Unpacker unpacker = msgpack.createUnpacker(in);
	    int size = unpacker.read(int.class);
	    this.arcops = new ArrayList<ArcOp>(size);
	    this.timestamp = unpacker.read(long.class);
	    for (int i=0; i<size;i++){
	    	this.arcops.add(new ArcOp(unpacker.read(byte[].class)));
	    }
	    ops2nodes();
	}
	public byte[] toMsg() throws IOException{
		update();
		MessagePack msgpack = MemoProxyBus.getBus().getMsgPack();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Packer packer = msgpack.createPacker(out);
		packer.write(this.arcops.size());
		packer.write(this.timestamp);
		for (ArcOp ops : this.arcops){
			packer.write(ops.toMsg());
		}
		return out.toByteArray();
	}
	
	long getTimestamp_long() {
		return this.timestamp;
	}

	void update(){
		if (isProxy){
			//TODO: Check for changed values
			this.arcops = proxyBus.getOps(nodeId, type);
			ops2nodes();
		} else {
			if (this.arcops == null || readBus.opsChanged(lastUpdate)) {
				this.arcops = readBus.getOps(nodeId, type);
				ops2nodes();
				lastUpdate = new Date().getTime();
			}			
		}
	}
	
	ArrayList<MemoNode> getNodes(long timestamp) {
		if (isProxy){
			this.arcops = proxyBus.getOps(nodeId, type, timestamp);
		} else {
			this.arcops = readBus.getOps(nodeId, type, timestamp);
		}
		ops2nodes();
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(this.nodes.length);
		for (UUID id : this.nodes) {
			if (proxyBus.isProxy(id)){
				result.add(proxyBus.find(id));
			} else {
				result.add(readBus.find(id));
			}
		}
		return result;
	}

	ArrayList<MemoNode> getNodes() {
		update();
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(this.nodes.length);
		for (UUID id : this.nodes) {
			result.add(new MemoNode(id));
		}
		return result;
	}

	int getLength() {
		update();
		return this.nodes.length;
	}

	void addNode(UUID other) {
		if (this.arcops == null){ //small performance gain
			update();
		}
		UUID[] arc = new UUID[2];
		arc[this.type] = other;
		arc[Math.abs(this.type - 1)] = this.nodeId;

		ArcOp op = new ArcOp(Ops.ADD, arc, new Date());
		if (isProxy){
			proxyBus.store(op);
		} else {
			writeBus.store(op);	
		}
		arcops.add(op);
	}

	void delNode(UUID other) {
		if (this.arcops == null){ //small performance gain
			update();
		}
		UUID[] arc = new UUID[2];
		arc[this.type] = other;
		arc[Math.abs(this.type - 1)] = this.nodeId;

		ArcOp op = new ArcOp(Ops.DELETE, arc, new Date());
		if (isProxy){
			proxyBus.store(op);
		} else {
			writeBus.store(op);	
		}
		arcops.add(op);
	}

	void clear() {
		update();
		for (UUID other : this.nodes) {
			this.delNode(other);
		}
	}

	private void ops2nodes() {
		HashSet<UUID> nodeList = new HashSet<UUID>(arcops.size());
		for (ArcOp op : arcops) {
			switch (op.getType()) {
			case ADD:
				nodeList.add(op.get(this.type));
				break;
			case DELETE:
				nodeList.remove(op.get(this.type));
				break;
			}
		}
		if (arcops.size() > 0) {
			this.timestamp = arcops.get(arcops.size() - 1).getTimestamp_long();
		}
		this.nodes = nodeList.toArray(new UUID[0]);
	}
}
