package com.chap.memo.memoNodes.NewImpl;

import java.util.Date;

import com.eaio.uuid.UUID;

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
	
	public NodeValue store(UUID id, byte[] value){
		NodeValue result = new NodeValue(id, value, new Date().getTime());
		values.store(result);
		if (values.nodes.size() >= NodeValueShard.SHARDSIZE){
				flushValues();
		}
		return result;
	}
	public void store(ArcOp op){
		ops.store(op);
		if (ops.children.size() >= ArcOpShard.SHARDSIZE){
			flushOps();
		}
	}
}
