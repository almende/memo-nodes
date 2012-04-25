package com.chap.memo.memoNodes.NewImpl;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.QueryResultList;

public class MemoReadBus {
	//Shard caches:
	static Map<Key, NodeValueShard> NodeValueShards = Collections
			.synchronizedMap(new MyMap<Key, NodeValueShard>(10, new Float(
					0.5), true));
	static Map<Key, ArcOpShard> ArcOpShards = Collections
			.synchronizedMap(new MyMap<Key, ArcOpShard>(10, new Float(
					0.5), true));
	ArrayList<NodeValueIndex> NodeValueIndexes = new ArrayList<NodeValueIndex>(100);
	ArrayList<ArcOpIndex> ArcOpIndexes = new ArrayList<ArcOpIndex>(100);
	PreparedQuery NodeValueIndexQuery;
	Cursor NodeValueCursor;
	PreparedQuery ArcOpIndexQuery;
	Cursor ArcOpCursor;
	DatastoreService datastore = null;
	
	
	private final static MemoReadBus bus = new MemoReadBus();
	
	public void loadIndexes(){
		if (datastore == null) datastore = DatastoreServiceFactory.getDatastoreService();
		
		Query q = new Query("NodeValueIndex").addSort("timestamp", SortDirection.DESCENDING);
		NodeValueIndexQuery = datastore.prepare(q);
		QueryResultList<Entity> rl = NodeValueIndexQuery.asQueryResultList(withLimit(100));
		if (rl.size() > 0) {
			NodeValueCursor = rl.getCursor();
			for (Entity ent : rl){
				NodeValueIndex index = (NodeValueIndex) MemoStorable.load(ent);
				NodeValueIndexes.add(index);
			}
		}
		
		q = new Query("ArcOpIndex").addSort("timestamp");
		ArcOpIndexQuery = datastore.prepare(q);
		rl = ArcOpIndexQuery.asQueryResultList(withLimit(1000));//TODO: what if more than 1000 ArcOpIndexes are stored?
		if (rl.size() > 0) {
			ArcOpCursor = rl.getCursor();
			for (Entity ent : rl){
				ArcOpIndex index = (ArcOpIndex) MemoStorable.load(ent);
				ArcOpIndexes.add(index);
			}
		}
	
	}
	
	private MemoReadBus(){
		loadIndexes();
	};
	
	public static MemoReadBus getBus(){
		return bus;
	}
	
	public boolean valueChanged(long timestamp){
		//TODO		
		return true;
	}
	public boolean opsChanged(long timestamp){
		//TODO
		return true;
	}
	
	public MemoNode find(UUID uuid){
		return new MemoNode(getValue(uuid));
	}
	public MemoNode find(UUID uuid,long timestamp){
		return new MemoNode(getValue(uuid,timestamp));
	}
	public ArrayList<MemoNode> findAll(UUID uuid){
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(100);
		Query q = new Query("NodeValueIndex").addSort("timestamp", SortDirection.DESCENDING);
		PreparedQuery pq = datastore.prepare(q);
		Iterator<Entity> iter = pq.asIterator();
		while (iter.hasNext()){
			NodeValueIndex index = (NodeValueIndex) MemoStorable.load(iter.next());
			if (index.nodeIds.contains(uuid)){
				NodeValueShard shard;
				synchronized(NodeValueShards){
					if (NodeValueShards.containsKey(index.shardKey)){
						shard = NodeValueShards.get(index.shardKey);
					} else {
						shard = (NodeValueShard) MemoStorable.load(index.shardKey); 
					}
				}
				NodeValueShards.put(shard.myKey,shard);
				for (NodeValue nv : shard.findAll(uuid)){
					//TODO:Moet eigenlijk nog sorteren op tijd
					result.add(new MemoNode(nv));
				}
			}
		}
		Collections.sort(result);
		return result;
	}
	public NodeValue getValue(UUID uuid){
		//TODO: performance verbeteren, op dit moment is het ongecached/ongeindexeerd in tijd opvragen voor elke Node!
		NodeValue result = null;
		MemoWriteBus writeBus = MemoWriteBus.getBus();
		result = writeBus.values.find(uuid);
		
		int indexCnt = 0;
		if (indexCnt >= NodeValueIndexes.size()) return result;
		NodeValueIndex index = NodeValueIndexes.get(indexCnt++);
		while (index != null && (result == null || index.newest > result.getTimestamp_long())){
			if (index.nodeIds.contains(uuid)){
				NodeValueShard shard=null;
				synchronized(NodeValueShards){
					if (NodeValueShards.containsKey(index.shardKey)){
						shard = NodeValueShards.get(index.shardKey);
					}
				}
				if (shard ==null) {
					shard = (NodeValueShard) MemoStorable.load(index.shardKey); 
				}
				NodeValueShards.put(shard.myKey,shard);
				NodeValue res = shard.find(uuid); 
				if (result == null || res.getTimestamp_long()>result.getTimestamp_long()){
					result = res;
				}
			}
			if (indexCnt >= NodeValueIndexes.size()){
				//TODO: load more indexes;
			} else {
				index = NodeValueIndexes.get(indexCnt++);
			}
		}
		return result;
	}
	public NodeValue getValue(UUID uuid,long timestamp){
		NodeValue result = null;

		MemoWriteBus writeBus = MemoWriteBus.getBus();
		result = writeBus.values.findBefore(uuid, timestamp);
		
		int indexCnt = 0;
		if (indexCnt >= NodeValueIndexes.size()) return result;
		NodeValueIndex index = NodeValueIndexes.get(indexCnt++);
		while (index != null && (result == null || index.newest > result.getTimestamp_long())){
			if (index.oldest < timestamp && index.nodeIds.contains(uuid)){
				NodeValueShard shard=null;
				synchronized(NodeValueShards){
					if (NodeValueShards.containsKey(index.shardKey)){
						shard = NodeValueShards.get(index.shardKey);
					}
				}
				if (shard ==null) {
					shard = (NodeValueShard) MemoStorable.load(index.shardKey); 
				}
				NodeValueShards.put(shard.myKey,shard);
				NodeValue res = shard.findBefore(uuid,timestamp); 
				if (result == null || res.getTimestamp_long()>result.getTimestamp_long()){
					result = res;
				}
			}
			if (indexCnt >= NodeValueIndexes.size()){
				//TODO: load more indexes;
			} else {
				index = NodeValueIndexes.get(indexCnt++);
			}
		}		
		return result;
	}
	public ArrayList<ArcOp> getOps(UUID uuid,int type){
		return getOps(uuid,type,new Date().getTime());
	}
	public ArrayList<ArcOp> getOps(UUID uuid, int type, long timestamp){
		ArrayList<ArcOp> result = new ArrayList<ArcOp>(100);	
		
		Iterator<ArcOpIndex> iter = ArcOpIndexes.iterator();
		while (iter.hasNext()){
			ArcOpIndex index = iter.next();
			switch (type){
			case 0: //parentList, UUID is child
				if (index.children.contains(uuid)){
					ArcOpShard shard=null;
					synchronized(NodeValueShards){
						if (ArcOpShards.containsKey(index.shardKey)){
							shard = ArcOpShards.get(index.shardKey);
						}
					}
					if (shard==null) {
						shard = (ArcOpShard) MemoStorable.load(index.shardKey); 
					}
					ArcOpShards.put(shard.myKey,shard);
					for (ArcOp op : shard.getChildOps(uuid)){
						if (op.getTimestamp_long()<= timestamp){
							result.add(op);
						}
					}	
				}
				break;
			case 1: //parentList, UUID is child
				if (index.parents.contains(uuid)){
					ArcOpShard shard=null;
					synchronized(NodeValueShards){
						if (ArcOpShards.containsKey(index.shardKey)){
							shard = ArcOpShards.get(index.shardKey);
						}
					}
					if (shard==null) {
						shard = (ArcOpShard) MemoStorable.load(index.shardKey); 
					}
					ArcOpShards.put(shard.myKey,shard);
					for (ArcOp op : shard.getParentOps(uuid)){
						if (op.getTimestamp_long()<= timestamp){
							result.add(op);
						}
					}	
				}
				break;
			}			
		}
		switch (type){
		case 0: //parentList, UUID is child
			for (ArcOp op : MemoWriteBus.getBus().ops.getChildOps(uuid)){
				if (op.getTimestamp_long()<= timestamp){
					result.add(op);
				}
			}
			break;
		case 1:
			for (ArcOp op : MemoWriteBus.getBus().ops.getParentOps(uuid)){
				if (op.getTimestamp_long()<= timestamp){
					result.add(op);
				}
			}
			break;			
		}
		Collections.sort(result);
		return result;
	}
	
}
