package com.chap.memo.memoNodes.NewImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.QueryResultList;
import static com.google.appengine.api.datastore.FetchOptions.Builder.*;

public class MemoReadBus {
	private final static MemoReadBus bus = new MemoReadBus();
	static final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
	private MemoReadBus(){};
	
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
				NodeValueShard shard = (NodeValueShard) MemoStorable.load(index.shardKey);
				for (NodeValue nv : shard.findAll(uuid)){
					//TODO:Moet eigenlijk nog sorteren op tijd
					result.add(new MemoNode(nv));
				}
			}
		}
		return result;
	}
	public NodeValue getValue(UUID uuid){
		//TODO: performance verbeteren, op dit moment is het ongecached/ongeindexeerd in tijd opvragen voor elke Node!
		
		Query q = new Query("NodeValueIndex").addSort("timestamp", SortDirection.DESCENDING);
		PreparedQuery pq = datastore.prepare(q);
		QueryResultList<Entity> rl = pq.asQueryResultList(withLimit(1));
		Cursor cu = rl.getCursor();
		
		NodeValueIndex index = (NodeValueIndex) MemoStorable.load(rl.get(0));
		NodeValue result = null;
		while (result == null || index.newest > result.getTimestamp_long()){
			if (index.nodeIds.contains(uuid)){
				NodeValueShard shard = (NodeValueShard) MemoStorable.load(index.shardKey);
				NodeValue res = shard.find(uuid); 
				if (result == null || res.getTimestamp_long()>result.getTimestamp_long()){
					result = res;
				}
			}
			rl = pq.asQueryResultList(withLimit(1).startCursor(cu));
			cu = rl.getCursor();
			index = (NodeValueIndex) MemoStorable.load(rl.get(0));
		}
		return result;
	}
	public NodeValue getValue(UUID uuid,long timestamp){
		//TODO: performance verbeteren, op dit moment is het ongecached/ongeindexeerd in tijd opvragen voor elke Node!
		Query q = new Query("NodeValueIndex").addSort("timestamp", SortDirection.DESCENDING);
		PreparedQuery pq = datastore.prepare(q);
		QueryResultList<Entity> rl = pq.asQueryResultList(withLimit(1));
		Cursor cu = rl.getCursor();
		
		NodeValueIndex index = (NodeValueIndex) MemoStorable.load(rl.get(0));
		NodeValue result = null;
		while (result == null || index.newest > result.getTimestamp_long()){
			if (index.oldest < timestamp && index.nodeIds.contains(uuid)){
				NodeValueShard shard = (NodeValueShard) MemoStorable.load(index.shardKey);
				NodeValue res = shard.find(uuid); 
				if (res.getTimestamp_long()<=timestamp && (result == null || res.getTimestamp_long()>result.getTimestamp_long())){
					result = res;
				}
			}
			rl = pq.asQueryResultList(withLimit(1).startCursor(cu));
			cu = rl.getCursor();
			index = (NodeValueIndex) MemoStorable.load(rl.get(0));
		}
		return result;
	}
	public ArrayList<ArcOp> getOps(UUID uuid,int type){
		return getOps(uuid,type,new Date().getTime());
	}
	public ArrayList<ArcOp> getOps(UUID uuid, int type, long timestamp){
		ArrayList<ArcOp> result = new ArrayList<ArcOp>(100);
		Query q = new Query("ArcOpIndex").addSort("timestamp").addFilter("timestamp", FilterOperator.LESS_THAN_OR_EQUAL, timestamp);
		PreparedQuery pq = datastore.prepare(q);
		Iterator<Entity> iter = pq.asIterator();
		while (iter.hasNext()){
			ArcOpIndex index = (ArcOpIndex) MemoStorable.load(iter.next());
			switch (type){
			case 0: //parentList, UUID is child
				if (index.children.contains(uuid)){
					ArcOpShard shard = (ArcOpShard) MemoStorable.load(index.shardKey);
					for (ArcOp op : shard.getChildOps(uuid)){
						if (op.getTimestamp_long()<= timestamp){
							result.add(op);
						}
					}	
				}
				break;
			case 1: //parentList, UUID is child
				if (index.parents.contains(uuid)){
					ArcOpShard shard = (ArcOpShard) MemoStorable.load(index.shardKey);
					for (ArcOp op : shard.getParentOps(uuid)){
						if (op.getTimestamp_long()<= timestamp){
							result.add(op);
						}
					}	
				}
				break;
			}
		}
		return result;
	}
	
}
