package com.chap.memo.memoNodes.model;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;

import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.bus.MemoProxyBus;
import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.bus.MemoWriteBus;
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

	public ArcList(UUID nodeId, int type, boolean isProxy) {
		this.type = type;
		this.nodeId = nodeId;
		this.isProxy = isProxy;
	}

	public ArcList(UUID nodeId,int type,byte[] msg) throws IOException{
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
	
	public long getTimestamp_long() {
		return this.timestamp;
	}

	public void update(){
		if (isProxy){
			//TODO: Check for changed values
			this.arcops = proxyBus.getOps(nodeId, type, 0);
			ops2nodes();
		} else {
			if (this.arcops == null || readBus.opsChanged(lastUpdate)) {
				if (this.arcops == null){
					this.arcops = readBus.getOps(nodeId, type, 0);	
				} else {
					this.arcops.addAll(readBus.getOps(nodeId, type, lastUpdate));
				}
				ops2nodes();
				lastUpdate = System.currentTimeMillis();
			}			
		}
	}
	
	public ArrayList<MemoNode> getNodes(long timestamp) {
		if (isProxy){
			this.arcops = proxyBus.getOps(nodeId, type, timestamp, 0);//TODO: also incremental?
		} else {
			if (this.arcops == null){
				this.arcops = readBus.getOps(nodeId, type, timestamp, 0);
			} else {
				this.arcops.addAll(readBus.getOps(nodeId, type, timestamp,lastUpdate));
			}
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

	public ArrayList<MemoNode> getNodes() {
		update();
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(this.nodes.length);
		for (UUID id : this.nodes) {
			result.add(new MemoNode(id));
		}
		return result;
	}

	public int getLength() {
		update();
		return this.nodes.length;
	}

	public void addNode(UUID other) {
		if (this.arcops == null){ //small performance gain
			update();
		}
		UUID[] arc = new UUID[2];
		arc[this.type] = other;
		arc[Math.abs(this.type - 1)] = this.nodeId;

		ArcOp op = new ArcOp(OpsType.ADD, arc, System.currentTimeMillis());
		if (isProxy){
			proxyBus.store(op);
		} else {
			writeBus.store(op);	
		}
		arcops.add(op);
	}

	public void delNode(UUID other) {
		if (this.arcops == null){ //small performance gain
			update();
		}
		UUID[] arc = new UUID[2];
		arc[this.type] = other;
		arc[Math.abs(this.type - 1)] = this.nodeId;

		ArcOp op = new ArcOp(OpsType.DELETE, arc, System.currentTimeMillis());
		if (isProxy){
			proxyBus.store(op);
		} else {
			writeBus.store(op);	
		}
		arcops.add(op);
	}

	public void clear() {
		update();
		for (UUID other : this.nodes) {
			this.delNode(other);
		}
	}

	private void ops2nodes() {
		if (arcops == null ) arcops = new ArrayList<ArcOp>(10); 
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
