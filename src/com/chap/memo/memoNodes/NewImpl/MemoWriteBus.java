package com.chap.memo.memoNodes.NewImpl;

import java.util.Date;

public class MemoWriteBus {
	private final static MemoWriteBus bus= new MemoWriteBus();
	private MemoWriteBus(){};
	
	public static MemoWriteBus getBus(){
		return bus;
	}
	NodeValueShard values = new NodeValueShard();
	ArcOpShard ops = new ArcOpShard();
	
	public void flush(){
		flushValues();
		flushOps();
	}
	
	public void flushValues(){
		new NodeValueIndex(values);
		values= new NodeValueShard();
	}
	public void flushOps(){
		new ArcOpIndex(ops);
		ops= new ArcOpShard();
	}
	
	public void store(MemoNode node){
		values.store(new NodeValue(node.getId(), node.getValue(), node.getTimestamp()));
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
