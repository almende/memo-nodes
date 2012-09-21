package com.chap.memo.memoNodes.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.MemoUtils;
import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.bus.MemoWriteBus;
import com.eaio.uuid.UUID;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ObjectArrays;

public class ArcList {
	MemoReadBus readBus = MemoReadBus.getBus();
	MemoWriteBus writeBus = MemoWriteBus.getBus();
	long lastUpdate = 0;

	// UUID[] nodes = new UUID[0];
	ArrayList<ArcOp> arcops = null;

	int type; // 0: parent list, 1:child list
	UUID nodeId;

	private long timestamp = 0;
	private static final UuidCmp cmp = new UuidCmp(); 
	
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
			boolean debug=false;
			if (nodeId.equals(new UUID("0003bc70-03f2-11e2-90bf-ac7289d75e0b"))){
				System.out.println("Calling getNodesIds for debug node:"+(this.arcops!=null?this.arcops:"null")+" : "+((MemoUtils.gettime(nodeId)-0x01B21DD213814000L)/10000)+" / "+lastUpdate);
				debug=true;
			}
			if (readBus.opsChanged(lastUpdate)) this.arcops = null;
			if (this.arcops == null) {
				this.arcops = readBus.getOps(nodeId, type, 0);
			}
//			if (this.arcops == null || readBus.opsChanged(lastUpdate)) {
//				if (this.arcops == null) {
//					this.arcops = readBus.getOps(nodeId, type, 0);
//				} else {
//					this.arcops
//							.addAll(readBus.getOps(nodeId, type, lastUpdate));
//				}
				lastUpdate = System.currentTimeMillis();
//			}
			if (arcops == null){
				if (debug) System.out.println("returning empty!");
				return new UUID[0];
			}
			HashMap<UUID,ArcOp> list = new HashMap<UUID,ArcOp>(arcops.size() / 2);
			if (debug) System.out.println("arcops size: "+arcops.size()+" "+arcops);
			for (ArcOp op : arcops) {
				if (op.timestamp > this.timestamp) this.timestamp = op.timestamp;
				if (op.getChild().equals(new UUID("0003bc70-03f2-11e2-90bf-ac7289d75e0b"))){
					System.out.println("Debug2:"+this.type+" = "+op.toString());
					//debug=true;
				}
				ArcOp otherOp = list.get(op.get(this.type));
				if (otherOp != null){
					if (debug) System.out.println("exists!");
					if (op.timestamp<otherOp.timestamp){
						if (debug) System.out.println("older!");
						continue;
					}					
				}
				if (debug) System.out.println("Adding!"+op.toString());
				list.put(op.get(this.type),op);
			}
			List<UUID> result = new ArrayList<UUID>(list.size());
			for (ArcOp op : list.values()){
				if (op.getType() == OpsType.ADD){
					result.add(op.get(this.type));
				}
			}
			Collections.sort(result,cmp);
			UUID[] res = result.toArray(new UUID[0]);
			if (result.size()>1){
				int pivot = Math.abs(Collections.binarySearch(result, nodeId, cmp));
				if (pivot < res.length){
					UUID[] front = Arrays.copyOfRange(res, pivot, res.length);
					UUID[] tail = Arrays.copyOf(res, pivot);
//					System.out.println("Pivot:"+pivot+"/"+res.length+":"+result.size()+" -> "+front.length+":"+tail.length);
					res = ObjectArrays.concat(front,tail,UUID.class);
				}
			}
			if (result.size() != res.length) System.out.println("Error: Lost a child!"+res.length+":"+result.size());
			return res; 
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
		if (nodeId.equals(new UUID("0003bc70-03f2-11e2-90bf-ac7289d75e0b"))){
			System.out.println("Debug: deleting node:"+op.toString());
		}
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
class UuidCmp implements Comparator<UUID>{
	public int compare(UUID a,UUID b) {
		return a.time==b.time?0:(MemoUtils.gettime(a)>MemoUtils.gettime(b)?1:-1);
	}
}