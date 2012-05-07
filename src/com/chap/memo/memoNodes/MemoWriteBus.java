package com.chap.memo.memoNodes;

import java.util.Date;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

public class MemoWriteBus {
	private final static MemoWriteBus bus= new MemoWriteBus();
	static final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
	NodeValueShard values;
	ArcOpShard ops;
	
	private MemoWriteBus(){
		values = new NodeValueShard();
		ops = new ArcOpShard();
	};
	
	protected static MemoWriteBus getBus(){
		return bus;
	}

	static protected void emptyDB() {
		// create one big cleanup query
		String[] types = { "NodeValueIndex","ArcOpIndex","NodeValueShard","ArcOpShard" };
		for (String type : types) {
			Query q = new Query(type).setKeysOnly();
			PreparedQuery pq = datastore.prepare(q);
			// int count = pq.countEntities();
			// System.out.println("Deleting :"+count+" entries of type:"+type);
			for (Entity res : pq.asIterable()) {
				datastore.delete(res.getKey());
				try {
					datastore.get(res.getKey());
				} catch (EntityNotFoundException e) {
				}
			}
		}
		MemoReadBus.getBus().loadIndexes(true,0);
		System.out.println("Database cleared!");
	}
	
	protected void flush(){
		flushValues();
		flushOps();
		MemoReadBus.getBus().updateIndexes();
	}
	
	protected void flushValues(){
		synchronized(values.nodes){
			if (values.nodes.size()>0){
				NodeValueIndex index = new NodeValueIndex(values);
				MemoReadBus.getBus().addValueIndex(index,values);
				values= new NodeValueShard();
			}
		}
	}
	protected void flushOps(){
		synchronized(ops){
			if (ops.currentSize>0){
				ArcOpIndex index = new ArcOpIndex(ops);
				MemoReadBus.getBus().addOpsIndex(index,ops);
				ops= new ArcOpShard();
			}
		}
	}
	
	protected NodeValue store(UUID id, byte[] value){
		long now = new Date().getTime();
		NodeValue result = new NodeValue(id, value, now);
		values.store(result);
		MemoReadBus.getBus().lastValueChange=now;
		if (values.nodes.size() >= NodeValueShard.SHARDSIZE){
				flushValues();
		}
		return result;
	}
	protected void store(ArcOp op){
		ops.store(op);
		MemoReadBus.getBus().lastOpsChange=new Date().getTime();
		if (ops.currentSize >= ArcOpShard.SHARDSIZE){
			flushOps();
		}
	}
}
