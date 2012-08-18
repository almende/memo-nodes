package com.chap.memo.memoNodes.bus;

import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.ArcOpBuffer;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.model.NodeValueBuffer;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

public class MemoWriteBus {
	private final static MemoWriteBus bus = new MemoWriteBus();
	static final DatastoreService datastore = DatastoreServiceFactory
			.getDatastoreService();
	
	MemoReadBus readBus;
	NodeValueBuffer values;
	ArcOpBuffer ops;
	
	private MemoWriteBus() {
		values = new NodeValueBuffer();
		ops = new ArcOpBuffer();
	};

	public static MemoWriteBus getBus() {
		return bus;
	}

	public void empty(){
		synchronized(ops){
			ops.children.clear();
			ops.parents.clear();
		}
		synchronized(values){
			values.nodes.clear();
		}
	}
	public static void emptyDB() {
		String[] types = { "NodeValueIndex", "ArcOpIndex", "NodeValueShard",
				"ArcOpShard" };
		for (String type : types) {
			Query q = new Query(type).setKeysOnly();
			PreparedQuery pq = datastore.prepare(q);
			for (Entity res : pq.asIterable()) {
				datastore.delete(res.getKey());
				try {
					datastore.get(res.getKey());
				} catch (EntityNotFoundException e) {
				}
			}
		}
		MemoWriteBus.getBus().empty();
		MemoReadBus.getBus().loadIndexes(true, 0);
		System.out.println("Database cleared!");
	}

	public void flush() {
		if (readBus == null){
			readBus = MemoReadBus.getBus();
		}
		ops.flush();
		values.flush();
		readBus.updateIndexes();
	}

	public NodeValue store(UUID id, byte[] value) {
		if (readBus == null){
			readBus = MemoReadBus.getBus();
		}
		NodeValue result = new NodeValue(id, value, System.currentTimeMillis());
		values.store(result);
		readBus.lastValueChange = System.currentTimeMillis();
		return result;
	}

	public void store(ArcOp op) {
		if (readBus == null){
			readBus = MemoReadBus.getBus();
		}
		ops.store(op);
		readBus.lastOpsChange = System.currentTimeMillis();
	}
}
