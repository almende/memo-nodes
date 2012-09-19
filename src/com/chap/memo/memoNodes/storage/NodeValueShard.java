package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.chap.memo.memoNodes.MemoUtils;
import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.model.NodeValueBuffer;
import com.eaio.uuid.UUID;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public final class NodeValueShard extends MemoStorable {
	private static final long serialVersionUID = 7295820980658238258L;
	final long oldest;
	final long newest;
	final NodeValue[] nodeArray;
	final int size;
	
	transient private final static UuidCmp nodeCmp = new UuidCmp();

	
	public static void devideAndMerge(NodeValueShard[] shards){
		if (shards.length <= 1) return;
		MemoReadBus ReadBus = MemoReadBus.getBus(); 
		int maxSize = NodeValueBuffer.COMPRESSION_RATIO*NodeValueBuffer.STORESIZE;
		ArrayList<NodeValue> nodes = new ArrayList<NodeValue>(maxSize);
		for (NodeValueShard shard : shards){
			nodes.ensureCapacity(nodes.size()+shard.nodeArray.length);
			nodes.addAll(Arrays.asList(shard.nodeArray));
		}
		Collections.sort(nodes); //Sort on Timestamp
		ArrayList<NodeValue> newNodes = new ArrayList<NodeValue>(Math.min(maxSize,nodes.size()));
		
		Iterator<NodeValue> iter = nodes.iterator();
		while (iter.hasNext()){
			int count = maxSize;
			NodeValue node = iter.hasNext()?iter.next():null;
			while (count > 0 && node != null){
				newNodes.add(node);
				iter.remove();
				count-=node.getValue().length+48;
				node = iter.hasNext()?iter.next():null;
			}
			if (count <= 0){
				NodeValueShard shard = new NodeValueShard(newNodes);
				NodeValueIndex index = new NodeValueIndex(shard);
				ReadBus.addNodeValueIndex(index, shard);
				newNodes.clear();
			}
			if (node!=null) newNodes.add(node);
		}
		if (newNodes.size() > 0){
			NodeValueShard shard = new NodeValueShard(newNodes);
			NodeValueIndex index = new NodeValueIndex(shard);
			ReadBus.addNodeValueIndex(index, shard);
			newNodes.clear();
		}
		for (NodeValueShard other : shards) {
			NodeValueIndex idx = ReadBus.removeNodeValueIndexByShard(other
					.getMyKey());
			if (idx != null)
				idx.delete();
			ReadBus.delShard(other);
			other.delete();
		}
	}
	public NodeValueShard(NodeValueBuffer buffer){
		List<NodeValue> list = new ArrayList<NodeValue>(buffer.nodes.values());	
		newest = buffer.getNewest();
		oldest = buffer.getOldest();
		Collections.sort(list);
		Collections.sort(list,nodeCmp);
		nodeArray=list.toArray(new NodeValue[0]);
		int count = 0;
		for (NodeValue val: nodeArray){
			count+=val.getValue().length+48;
		}
		size=count;
	}
	public NodeValueShard(ArrayList<NodeValue> nodeList){
		Collections.sort(nodeList);
		oldest = nodeList.get(0).getTimestamp_long();
		newest = nodeList.get(nodeList.size()-1).getTimestamp_long();
		
		Collections.sort(nodeList,nodeCmp);
		nodeArray = nodeList.toArray(new NodeValue[0]);
		int count = 0;
		for (NodeValue val: nodeArray){
			count+=val.getValue().length+48;
		}
		size=count;
	}
	public ImmutableList<NodeValue> findAll(UUID id) {
		int pivot = Arrays.binarySearch(nodeArray,new NodeValue(id,null,0),nodeCmp);
		if (pivot<0) return ImmutableList.of();
		while (pivot>0 && nodeArray[pivot-1].getId().time == id.time) pivot--;
		Builder<NodeValue> resBuilder = ImmutableList.builder();
		while (pivot<nodeArray.length && nodeArray[pivot].getId().time == id.time){
			NodeValue val = nodeArray[pivot];
			if (val.getId().equals(id)){
				resBuilder.add(val);
			}
			pivot++;
		}
		return resBuilder.build();
	}

	public NodeValue find(UUID id) {
		return findBefore(id, System.currentTimeMillis());
	}

	public NodeValue findBefore(UUID id, Date timestamp) {
		return findBefore(id, timestamp.getTime());
	}

	public NodeValue findBefore(UUID id, long timestamp_long) {
		if (timestamp_long < oldest){
			System.out.println("Took shortcut! ??");
			return null; // shortcut, will probably not be used...
		}		
		int pivot = Arrays.binarySearch(nodeArray, new NodeValue(id,null,0),nodeCmp);
		if (pivot<0){
			return null;
		}
		while (pivot<nodeArray.length-1 && nodeArray[pivot+1].getId().time == id.time) pivot++;
		while (pivot>=0 && nodeArray[pivot].getId().time == id.time){
			NodeValue val = nodeArray[pivot];
			if (val.getId().equals(id) && val.getTimestamp_long()<=timestamp_long){
				return val;
			}
			pivot--;
		}
		return null;
	}

	public NodeValue[] getNodes(){
		return this.nodeArray;
	}
	public long getOldest() {
		return oldest;
	}

	public long getNewest() {
		return newest;
	}
	@Override
	public int getSize(){
		return this.size;
	}
	
}
class UuidCmp implements Comparator<NodeValue>{
	public int compare(NodeValue a,NodeValue b) {
		return a.getId().time==b.getId().time?0:(MemoUtils.gettime(a.getId())>MemoUtils.gettime(b.getId())?1:-1);
	}
}