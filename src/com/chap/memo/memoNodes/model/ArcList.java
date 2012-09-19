package com.chap.memo.memoNodes.model;

import java.util.ArrayList;
import java.util.HashMap;

import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.bus.MemoWriteBus;
import com.eaio.uuid.UUID;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class ArcList {
	MemoReadBus readBus = MemoReadBus.getBus();
	MemoWriteBus writeBus = MemoWriteBus.getBus();
	long lastUpdate = 0;

	// UUID[] nodes = new UUID[0];
	ArrayList<ArcOp> arcops = null;

	int type; // 0: parent list, 1:child list
	UUID nodeId;

	private long timestamp = 0;
	
	public ArcList(UUID nodeId, int type) {
		this.type = type;
		this.nodeId = nodeId;
	}

	public long getTimestamp_long() {
		return timestamp;
	}

	public ImmutableList<MemoNode> getNodes() {
		Builder<MemoNode> listBuilder = new ImmutableList.Builder<MemoNode>();
		for (UUID id : getNodesIds()) {
			listBuilder.add(new MemoNode(id));
		}
		return listBuilder.build();
	}	

	public UUID[] getNodesIds(){
		synchronized (this) {
			if (this.arcops == null || readBus.opsChanged(lastUpdate)) {
				if (this.arcops == null) {
					this.arcops = readBus.getOps(nodeId, type, 0);
				} else {
					this.arcops
							.addAll(readBus.getOps(nodeId, type, lastUpdate));
				}
				lastUpdate = System.currentTimeMillis();
			}
			if (arcops == null){
				arcops = new ArrayList<ArcOp>(10);
				return new UUID[0];
			}
			HashMap<UUID,ArcOp> list = new HashMap<UUID,ArcOp>(arcops.size() / 2);
			for (ArcOp op : arcops) {
				if (op.timestamp > this.timestamp) this.timestamp = op.timestamp;
				ArcOp otherOp = list.get(op.get(this.type));
				if (otherOp != null){
					if (op.timestamp<otherOp.timestamp){
						continue;
					}					
				}
				list.put(op.get(this.type),op);
			}
			ArrayList<UUID> result = new ArrayList<UUID>(list.size());
			for (ArcOp op : list.values()){
				if (op.getType() == OpsType.ADD){
					result.add(op.get(this.type));
				}
			}
			return result.toArray(new UUID[0]);
		}
		
	}
	public void addNode(UUID other) {
		if (this.arcops == null) {
			getNodesIds();
		}
		UUID[] arc = new UUID[2];
		arc[this.type] = other;
		arc[Math.abs(this.type - 1)] = this.nodeId;

		ArcOp op = new ArcOp(OpsType.ADD, arc, System.currentTimeMillis());
		writeBus.store(op);
		synchronized (this) {
			arcops.add(op);
		}
	}

	public void delNode(UUID other) {
		if (this.arcops == null) { 
			getNodesIds();
		}
		UUID[] arc = new UUID[2];
		arc[this.type] = other;
		arc[Math.abs(this.type - 1)] = this.nodeId;

		ArcOp op = new ArcOp(OpsType.DELETE, arc, System.currentTimeMillis());
		writeBus.store(op);
		synchronized (this) {
			arcops.add(op);
		}
	}
	public int getLength() {
		return getNodesIds().length;
	}

	public void clear() {
		UUID[] nodes = getNodesIds();
		arcops.ensureCapacity(arcops.size() + nodes.length);
		for (UUID other : nodes) {
			this.delNode(other);
		}
	}	
}
