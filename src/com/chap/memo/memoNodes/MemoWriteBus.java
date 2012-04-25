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
	
	public static MemoWriteBus getBus(){
		return bus;
	}

	
	static public void emptyDB() {
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
	
	public void flush(){
		flushValues();
		flushOps();
	}
	
	public void flushValues(){
		NodeValueIndex index = new NodeValueIndex(values);
		MemoReadBus.getBus().NodeValueIndexes.add(0, index);//At the start of the array
		values= new NodeValueShard();
	}
	public void flushOps(){
		ArcOpIndex index = new ArcOpIndex(ops);
		MemoReadBus.getBus().ArcOpIndexes.add(index);//to the end of the array
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
