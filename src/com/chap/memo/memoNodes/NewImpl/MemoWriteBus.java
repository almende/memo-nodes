package com.chap.memo.memoNodes.NewImpl;

import java.util.Date;

import com.chap.memo.memoNodes.MemoNode;

public class MemoWriteBus {
	NodeValueShard values = new NodeValueShard();
	ArcOpShard ops = new ArcOpShard();
	
	public void flush(){
		flushValues();
		flushOps();
	}
	
	public void flushValues(){
		//TODO: check for existing "small" shards, merge. (use as smart clustering as cheaply available)
		NodeValueIndex valIndex = new NodeValueIndex(values);
		//TODO: where to leave this index? Generate commit point
		values= new NodeValueShard();
	}
	public void flushOps(){
		//TODO: check for existing "small" shards, merge.
		ArcOpIndex opIndex = new ArcOpIndex(ops);
		//TODO: where to leave this index? Generate commit point 
		ops= new ArcOpShard();
	}
	
	public void store(MemoNode node){
		values.store(new NodeValue(node.getId(), node.getValue().getBytes(), node.getTimestamp()));
		if (values.nodes.size() >= NodeValueShard.SHARDSIZE){
			flushValues();
		}
	}
	public void addChild(MemoNode node, MemoNode child, Date timestamp){
		ops.store(new ArcOp(Ops.ADD,node.getId(),child.getId(),timestamp));
		if (ops.children.size() >= ArcOpShard.SHARDSIZE){
			flushOps();
		}
	}
	public void addParent(MemoNode node, MemoNode parent, Date timestamp){
		ops.store(new ArcOp(Ops.ADD,parent.getId(),node.getId(),timestamp));
		if (ops.children.size() >= ArcOpShard.SHARDSIZE){
			flushOps();
		}
	}
	public void delChild(MemoNode node, MemoNode child, Date timestamp){
		ops.store(new ArcOp(Ops.DELETE,node.getId(),child.getId(),timestamp));
		if (ops.children.size() >= ArcOpShard.SHARDSIZE){
			flushOps();
		}
	}
	public void delParent(MemoNode node, MemoNode parent, Date timestamp){
		ops.store(new ArcOp(Ops.DELETE,parent.getId(),node.getId(),timestamp));
		if (ops.children.size() >= ArcOpShard.SHARDSIZE){
			flushOps();
		}
	}
}
