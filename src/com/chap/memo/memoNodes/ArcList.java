package com.chap.memo.memoNodes;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import com.eaio.uuid.UUID;

public class ArcList {
	MemoReadBus readBus = MemoReadBus.getBus();
	MemoWriteBus writeBus = MemoWriteBus.getBus();
	long lastUpdate = 0;
	
	UUID[] nodes = new UUID[0];
	ArrayList<ArcOp> arcops = null;
	int type; //0: parent list, 1:child list
	UUID nodeId;
	
	private long timestamp;
	
	 ArcList(UUID nodeId,int type){
		this.type=type;
		this.nodeId=nodeId;
	}
	 long getTimestamp_long(){
		return this.timestamp;
	}
	 ArrayList<MemoNode> getNodes(long timestamp){
		this.arcops=readBus.getOps(nodeId,type,timestamp);
		ops2nodes();
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.nodes.length);
		for (UUID id : this.nodes) {
			result.add(readBus.find(id));
		}
		return result;
	}
	 ArrayList<MemoNode> getNodes(){
		if (this.arcops == null || readBus.opsChanged(lastUpdate)){
			this.arcops=readBus.getOps(nodeId,type);
			ops2nodes();
			lastUpdate=new Date().getTime();
		}
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.nodes.length);
		for (UUID id : this.nodes) {
			result.add(new MemoNode(id));
		}
		return result;
	}
	 int getLength(){
		if (this.arcops == null || readBus.opsChanged(lastUpdate)){
			this.arcops=readBus.getOps(nodeId,type);
			ops2nodes();
			lastUpdate=new Date().getTime();
		}
		return this.nodes.length;
	}
	 void addNode(UUID other){
		if (this.arcops == null){
			this.arcops=readBus.getOps(nodeId,type);
			lastUpdate=new Date().getTime();
		}
		UUID[] arc = new UUID[2];
		arc[this.type]=other;
		arc[Math.abs(this.type-1)]=this.nodeId;
		
		ArcOp op=new ArcOp(Ops.ADD, arc, new Date());
		writeBus.store(op);
		arcops.add(op);
	}
	 void delNode(UUID other){
		if (this.arcops == null){
			this.arcops=readBus.getOps(nodeId,type);
			lastUpdate=new Date().getTime();
		}
		UUID[] arc = new UUID[2];
		arc[this.type]=other;
		arc[Math.abs(this.type-1)]=this.nodeId;

		ArcOp op=new ArcOp(Ops.DELETE,arc, new Date());
		writeBus.store(op);
		arcops.add(op);
	}

	 void clear(){
		this.getNodes();//make sure all updates are known.
		for (UUID other: this.nodes){
			this.delNode(other);
		}
	}
	private void ops2nodes(){
		HashSet<UUID> nodeList = new HashSet<UUID>(arcops.size());
		for (ArcOp op : arcops){
			switch(op.getType()){
			case ADD:
				nodeList.add(op.get(this.type));
				break;
			case DELETE:
				nodeList.remove(op.get(this.type));
				break;
			}
		}
		if (arcops.size()>0){
			this.timestamp = arcops.get(arcops.size()-1).getTimestamp_long();
		}
		this.nodes=nodeList.toArray(new UUID[0]);
	}
}
