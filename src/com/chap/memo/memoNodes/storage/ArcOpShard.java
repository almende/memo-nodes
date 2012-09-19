package com.chap.memo.memoNodes.storage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

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
				if (parent != null) newParents.add(parent);
				if (child != null) newChildren.add(child);
			}
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
				if (parent != null) newRootParents.add(parent);
				if (child != null) newRootChildren.add(child);
			}
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
		
	public ArcOpShard(ArrayList<ArcOp> parentList,ArrayList<ArcOp> childList,ArrayList<ArcOp> rootParentList, ArrayList<ArcOp> rootChildList){
		parentArray = parentList.toArray(new ArcOp[0]);
		childArray = childList.toArray(new ArcOp[0]);
		rootParentArray = rootParentList.toArray(new ArcOp[0]);
		rootChildArray = rootChildList.toArray(new ArcOp[0]);
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
	}
	public ImmutableList<ArcOp> getChildOps(UUID id) {
		if (id.time == 0){
			return ImmutableList.copyOf(rootChildArray);
		}
		int pivot = Arrays.binarySearch(childArray, new ArcOp(OpsType.ADD,id,id,0),childCmp);
		if (pivot<0) {
			System.out.println("Ops not found:"+pivot);
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
			System.out.println("Ops not found:"+pivot);
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
