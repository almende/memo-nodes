package com.chap.memo.memoNodes.NewImpl;

import com.eaio.uuid.UUID;

public class MemoReadBus {
	private final static MemoReadBus bus = new MemoReadBus();
	private MemoReadBus(){};
	
	public static MemoReadBus getBus(){
		return bus;
	}
	
	public MemoNode find(UUID uuid){
		//TODO
		return null;
	}
}
