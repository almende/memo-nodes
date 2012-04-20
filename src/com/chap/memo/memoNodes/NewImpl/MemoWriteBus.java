package com.chap.memo.memoNodes.NewImpl;

import java.util.Date;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

public class MemoWriteBus {
	private final static MemoWriteBus bus= new MemoWriteBus();
	static final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
	private MemoWriteBus(){};
	
	public static MemoWriteBus getBus(){
		return bus;
	}
	NodeValueShard values = new NodeValueShard();
	ArcOpShard ops = new ArcOpShard();
	
	static public void emptyDB() {
		// create one big cleanup query
		String[] types = { "ArcOpIndex","ArcOpShard","NodeValueIndex","NodeValueShard" };
		for (String type : types) {
			Query q = new Query(type).setKeysOnly();
			PreparedQuery pq = datastore.prepare(q);
			// int count = pq.countEntities();
			// System.out.println("Deleting :"+count+" entries of type:"+type);
			for (Entity res : pq.asIterable()) {
				datastore.delete(res.getKey());
			}
		}
		System.out.println("Database cleared!");
	}
	
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
