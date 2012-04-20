package com.chap.memo.memoNodes.NewImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.eaio.uuid.UUID;

public class ArcList {
	MemoReadBus readBus = MemoReadBus.getBus();
	UUID[] nodes;
	
	private long timestamp;
	public long getTimestamp_long(){
		return this.timestamp;
	}
	public ArrayList<MemoNode> getNodes(){
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(
				this.nodes.length);
		List<UUID> parents = Arrays.asList(this.nodes);
		for (UUID id : parents) {
			result.add(readBus.find(id));
		}
		return result;	
	}
	public int getLength(){
		return this.nodes.length;
	}
}
