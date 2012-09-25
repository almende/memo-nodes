package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import com.chap.memo.memoNodes.bus.MemoReadBus;
import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.ArcOpBuffer;
import com.chap.memo.memoNodes.model.OpsType;
import com.eaio.uuid.UUID;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ObjectArrays;

public final class ArcOpShard extends MemoStorable {
	private static final long serialVersionUID = 7712775430540649570L;
	final ArcOp[] parentArray;
	final ArcOp[] childArray;
	final ArcOp[] rootParentArray;
	final ArcOp[] rootChildArray;
	
	transient private static final ChildCmp childCmp = new ChildCmp();
	transient private static final ParentCmp parentCmp = new ParentCmp();

	private static void addCmp (ArcOp op,HashMap<UUIDTuple,ArcOp> map){
		UUIDTuple tup = new UUIDTuple(op.getParent(),op.getChild());
		if (!map.containsKey(tup) || map.get(tup).getTimestamp_long()< op.getTimestamp_long()){
			map.put(tup,op);
		}
	}
	public static void dropHistory(ArcOpShard shard) {
		MemoReadBus ReadBus = MemoReadBus.getBus();
		
		System.out.println("Before:"+shard.parentArray.length+":"+shard.childArray.length+":"+shard.rootParentArray.length+":"+shard.rootChildArray.length);
		LinkedHashMap<UUIDTuple,ArcOp> map = new LinkedHashMap<UUIDTuple,ArcOp>(shard.getSize()/(2* ArcOpBuffer.BYTESPEROP));
		for (ArcOp op : shard.parentArray){
			addCmp(op,map);
		}
		List<ArcOp> parents = ImmutableList.copyOf(map.values());
		map.clear();
		for (ArcOp op : shard.rootParentArray){
			addCmp(op,map);
		}
		List<ArcOp> rootParents = ImmutableList.copyOf(map.values());
		map.clear();
		for (ArcOp op : shard.childArray){
			addCmp(op,map);
		}
		List<ArcOp> children = ImmutableList.copyOf(map.values());
		map.clear();
		for (ArcOp op : shard.rootChildArray){
			addCmp(op,map);
		}
		List<ArcOp> rootChildren = ImmutableList.copyOf(map.values());
		
		ArcOpShard newShard = new ArcOpShard(parents,children,rootParents,rootChildren);
		ArcOpIndex index = new ArcOpIndex(newShard);
		ReadBus.addOpsIndex(index, newShard);
		System.out.println("After:"+newShard.parentArray.length+":"+newShard.childArray.length+":"+newShard.rootParentArray.length+":"+newShard.rootChildArray.length);
		
		ArcOpIndex idx = ReadBus.removeArcOpIndexByShard(shard
				.getMyKey());
		if (idx != null) idx.delete();
		ReadBus.delShard(shard);
		shard.delete();
	}
	
	public static void devideAndMerge(ArcOpShard[] shards){
		if (shards.length <= 1) return;
		System.out.println("Merging "+shards.length+" arcOp shards");
		MemoReadBus ReadBus = MemoReadBus.getBus(); 
		ArrayList<ArcOp> rootParentList = new ArrayList<ArcOp>();
		ArrayList<ArcOp> rootChildList = new ArrayList<ArcOp>();
		ArrayList<ArcOp> parents = new ArrayList<ArcOp>();
		ArrayList<ArcOp> children = new ArrayList<ArcOp>();
		for (ArcOpShard shard : shards){
			parents.ensureCapacity(parents.size()+shard.parentArray.length);
			parents.addAll(Arrays.asList(shard.parentArray));
			children.ensureCapacity(children.size()+shard.childArray.length);
			children.addAll(Arrays.asList(shard.childArray));
			rootParentList.ensureCapacity(rootParentList.size()+shard.rootParentArray.length);
			rootParentList.addAll(Arrays.asList(shard.rootParentArray));
			rootChildList.ensureCapacity(rootChildList.size()+shard.rootChildArray.length);
			rootChildList.addAll(Arrays.asList(shard.rootChildArray));
		}
		Collections.sort(parents,parentCmp); 
		Collections.sort(children,childCmp); 
		
		int maxSize = ArcOpBuffer.STORESIZE*ArcOpBuffer.COMPRESSION_RATIO/ArcOpBuffer.BYTESPEROP;
		ArrayList<ArcOp> newParents = new ArrayList<ArcOp>(Math.min(maxSize,parents.size()));
		ArrayList<ArcOp> newChildren = new ArrayList<ArcOp>(Math.min(maxSize,children.size()));
		
		Iterator<ArcOp> parIter = parents.iterator();
		Iterator<ArcOp> chldIter = children.iterator();
		while (parIter.hasNext() || chldIter.hasNext()){
			int count = maxSize-(newParents.size()+newChildren.size());
			ArcOp parent = parIter.hasNext()?parIter.next():null;
			ArcOp child = chldIter.hasNext()?chldIter.next():null;
			while (count > 0 && parent != null && (child == null || parent.getParentTime()<= child.getChildTime())){
				newParents.add(parent);
				parIter.remove();
				parent = parIter.hasNext()?parIter.next():null;
				count--;
			}
			while (count > 0 && child != null && (parent == null || child.getChildTime() <= parent.getParentTime())){
				newChildren.add(child);
				chldIter.remove();
				child = chldIter.hasNext()?chldIter.next():null;
				count--;
			}
			if (count <= 0){
				ArcOpShard shard = new ArcOpShard(newParents,newChildren,new ArrayList<ArcOp>(0),new ArrayList<ArcOp>(0));
				ArcOpIndex index = new ArcOpIndex(shard);
				ReadBus.addOpsIndex(index, shard);
				newParents.clear();
				newChildren.clear();
			}
			if (parent != null) newParents.add(parent);
			if (child != null) newChildren.add(child);
		}
		ArrayList<ArcOp> newRootParents = new ArrayList<ArcOp>(Math.min(maxSize,rootParentList.size()));
		ArrayList<ArcOp> newRootChildren = new ArrayList<ArcOp>(Math.min(maxSize,rootChildList.size()));
		parIter = rootParentList.iterator();
		chldIter = rootChildList.iterator();
		while (parIter.hasNext() || chldIter.hasNext()){
			ArcOp parent = parIter.hasNext()?parIter.next():null;
			ArcOp child = chldIter.hasNext()?chldIter.next():null;
			int count = maxSize-(newParents.size()+newChildren.size())-(newRootParents.size()+newRootChildren.size());
			while(count>0 && parent != null){
				newRootParents.add(parent);
				parent = parIter.hasNext()?parIter.next():null;
				count--;
			}
			while(count>0 && child != null){
				newRootChildren.add(child);
				child = chldIter.hasNext()?chldIter.next():null;
				count--;
			}
			if (count<=0){
				ArcOpShard shard = new ArcOpShard(newParents,newChildren,newRootParents,newRootChildren);
				ArcOpIndex index = new ArcOpIndex(shard);
				ReadBus.addOpsIndex(index, shard);
				newParents.clear();
				newChildren.clear();
				newRootParents.clear();
				newRootChildren.clear();
			}
			if (parent != null) newRootParents.add(parent);
			if (child != null) newRootChildren.add(child);
		}
		if (newParents.size()>0 || newChildren.size()>0||newRootParents.size()>0||newRootChildren.size()>0){
			ArcOpShard shard = new ArcOpShard(newParents,newChildren,newRootParents,newRootChildren);
			ArcOpIndex index = new ArcOpIndex(shard);
			ReadBus.addOpsIndex(index, shard);
		}
		for (ArcOpShard other : shards) {
			ArcOpIndex idx = ReadBus.removeArcOpIndexByShard(other
					.getMyKey());
			if (idx != null)
				idx.delete();
			ReadBus.delShard(other);
			other.delete();
		}
	}
	public void setSpread(){
		long parTime = parentArray.length>0?parentArray[parentArray.length-1].getParentTime():0;
		long chldTime = childArray.length>0?childArray[childArray.length-1].getChildTime():0;
		long newest = Math.max(parTime, chldTime);
		parTime = parentArray.length>0?parentArray[0].getParentTime():newest;
		chldTime = childArray.length>0?childArray[0].getChildTime():newest;
		long oldest = Math.min(parTime, chldTime);
		this.storeTime = newest;
		this.spread = newest-oldest;
	}
	public ArcOpShard(List<ArcOp> parentList,List<ArcOp> childList,List<ArcOp> rootParentList, List<ArcOp> rootChildList){
		parentArray = parentList.toArray(new ArcOp[0]);
		childArray = childList.toArray(new ArcOp[0]);
		rootParentArray = rootParentList.toArray(new ArcOp[0]);
		rootChildArray = rootChildList.toArray(new ArcOp[0]);
		setSpread();
	}
	public ArcOpShard(ArcOpBuffer buffer){
		ArcOp[] tmp = buffer.getParentOps().toArray(new ArcOp[0]);
		Arrays.sort(tmp,parentCmp);
		int i=0;
		while(tmp[i].getParent().time == 0)i++;
		if (i>0){
			rootParentArray = Arrays.copyOf(tmp,i);
			parentArray = Arrays.copyOfRange(tmp,i,tmp.length);
		} else {
			rootParentArray = new ArcOp[0];
			parentArray = tmp;
		}
		tmp = buffer.getChildOps().toArray(new ArcOp[0]);
		Arrays.sort(tmp,childCmp);
		i=0;
		while(tmp[i].getChild().time==0)i++;
		if (i>0){
			rootChildArray = Arrays.copyOf(tmp,i);
			childArray = Arrays.copyOfRange(tmp,i,tmp.length);
		} else {
			rootChildArray = new ArcOp[0];
			childArray = tmp;		
		}
		setSpread();
	}
	public ImmutableList<ArcOp> getChildOps(UUID id) {
		if (id.time == 0){
			return ImmutableList.copyOf(rootChildArray);
		}
		int pivot = Arrays.binarySearch(childArray, new ArcOp(OpsType.ADD,id,id,0),childCmp);
		if (pivot<0) {
//			System.out.println("Ops not found:"+pivot);
			return ImmutableList.of();
		}
		Builder<ArcOp> resBuilder = ImmutableList.builder();
		while (pivot>0 && childArray[pivot-1].getChild().time == id.time) pivot--;
		while (pivot<childArray.length && childArray[pivot].getChild().time == id.time){
			ArcOp op = childArray[pivot];
			if (op.getChild().equals(id)){
				resBuilder.add(op);
			}
			pivot++;
		}
		return resBuilder.build();
	}

	public ImmutableList<ArcOp> getParentOps(UUID id) {
		if (id.time == 0){
			return ImmutableList.copyOf(rootParentArray);
		}
		int pivot = Arrays.binarySearch(parentArray, new ArcOp(OpsType.ADD,id,id,0),parentCmp);
		if (pivot<0) {
//			System.out.println("Ops not found:"+pivot);
			return ImmutableList.of();
		}
		Builder<ArcOp> resBuilder = ImmutableList.builder();
		while (pivot>0 && parentArray[pivot-1].getParent().time == id.time) pivot--;
		while (pivot<parentArray.length && parentArray[pivot].getParent().time == id.time){
			ArcOp op = parentArray[pivot];
			if (op.getParent().equals(id)){
				resBuilder.add(op);
			}
			pivot++;
		}
		return resBuilder.build();
	}
	public ArcOp[] getOps(){
		return ObjectArrays.concat(childArray,rootChildArray,ArcOp.class);
	}
	
	public boolean hasParentRoot(){
		return rootParentArray.length>0;
	}
	public boolean hasChildRoot(){
		return rootChildArray.length>0;
	}
	@Override
	public int getSize(){
		return (parentArray.length+rootParentArray.length+childArray.length+rootChildArray.length) * ArcOpBuffer.BYTESPEROP;
	}
}
class ParentCmp implements Comparator<ArcOp>{
	public int compare(ArcOp a,ArcOp b) {
		return a.getParent().time==b.getParent().time?0:(a.getParentTime()>b.getParentTime()?1:-1);
	}
}
class ChildCmp implements Comparator<ArcOp>{
	public int compare(ArcOp a,ArcOp b) {
		return a.getChild().time==b.getChild().time?0:(a.getChildTime()>b.getChildTime()?1:-1);
	}
}
class UUIDTuple {
	public UUID parent=null;
	public UUID child =null;
	public UUIDTuple(UUID parent, UUID child){
		this.parent=parent;
		this.child=child;
	}
	public int hashCode(){
		return (parent.hashCode()+child.hashCode())%Integer.MAX_VALUE;
	}
	public boolean equals(Object o){
		if (o instanceof UUIDTuple){
			UUIDTuple other = (UUIDTuple) o;
			if (other.parent.equals(parent) && other.child.equals(child)) return true;
		}
		return false;
	}
}
