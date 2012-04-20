package com.chap.memo.memoNodes.NewImpl;

import java.util.ArrayList;

import com.eaio.uuid.UUID;

public class MemoReadBus {
	private final static MemoReadBus bus = new MemoReadBus();
	private MemoReadBus(){};
	
	public static MemoReadBus getBus(){
		return bus;
	}
	
	public boolean valueChanged(long timestamp){
		return false;
	}
	public boolean opsChanged(long timestamp){
		return false;
	}
	
	public MemoNode find(UUID uuid){
		//TODO
		//Find newest NodeValue
		//Find newest Ops
		return null;
	}
	public MemoNode find(UUID uuid,long timestamp){
		//TODO
		//Find at timestamp
		//Find at timestamp
		return null;
	}
	public NodeValue getValue(UUID uuid){
		//TODO
		return null;
	}
	public NodeValue getValue(UUID uuid,long timestamp){
		//TODO
		return null;
	}
	public ArrayList<ArcOp> getOps(UUID uuid){
		//TODO
		return new ArrayList<ArcOp>(0);
	}
	public ArrayList<ArcOp> getOps(UUID uuid, long timestamp){
		//TODO
		return new ArrayList<ArcOp>(0);
	}
	
}
