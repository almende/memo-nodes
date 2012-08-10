package com.chap.memo.memoNodes.bus;

import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.storage.ArcOpIndex;
import com.chap.memo.memoNodes.storage.ArcOpShard;
import com.chap.memo.memoNodes.storage.NodeValueIndex;
import com.chap.memo.memoNodes.storage.NodeValueShard;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

public class MemoWriteBus {
	private final static MemoWriteBus bus = new MemoWriteBus();
	static final DatastoreService datastore = DatastoreServiceFactory
			.getDatastoreService();
	NodeValueShard values;
	ArcOpShard ops;
	MemoReadBus ReadBus;
	MemcacheService memCache = null;
	
	private MemoWriteBus() {
		values = new NodeValueShard();
		ops = new ArcOpShard();
	};

	public static MemoWriteBus getBus() {
		return bus;
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
		MemoReadBus.getBus().loadIndexes(true, 0);
		System.out.println("Database cleared!");
	}

	public void flush() {
		if (ReadBus == null){
			ReadBus = MemoReadBus.getBus();
		}
		flushValues();
		flushOps();
		ReadBus.updateIndexes();
	}

	private void flushValues() {
		if (memCache == null){
			memCache = MemcacheServiceFactory.getMemcacheService();
		}
		synchronized (values.getNodes()) {
			if (values.getNodes().size() > 0) {
				NodeValueIndex index = new NodeValueIndex(values);
				ReadBus.addValueIndex(index, values);
				values = new NodeValueShard();
				
				memCache.put("memoNodes_lastUpdate",System.currentTimeMillis());
			}
		}
	}

	private void flushOps() {
		if (memCache == null){
			memCache = MemcacheServiceFactory.getMemcacheService();
		}
		synchronized (ops) {
			if (ops.getCurrentSize() > 0) {
				ArcOpIndex index = new ArcOpIndex(ops);
				ReadBus.addOpsIndex(index, ops);
				ops = new ArcOpShard();
				
				memCache.put("memoNodes_lastUpdate",System.currentTimeMillis());
			}
		}
	}

	public NodeValue store(UUID id, byte[] value) {
		long now = System.currentTimeMillis();
		if (ReadBus == null){
			ReadBus = MemoReadBus.getBus();
		}
		NodeValue result = new NodeValue(id, value, now);
		values.store(result);
		ReadBus.lastValueChange = now;
		if (values.getCurrentSize() >= NodeValueShard.SHARDSIZE) {
			flushValues();
		}
		return result;
	}

	public void store(ArcOp op) {
		ops.store(op);
		if (ReadBus == null){
			ReadBus = MemoReadBus.getBus();
		}
		ReadBus.lastOpsChange = System.currentTimeMillis();
		if (ops.getCurrentSize() >= ArcOpShard.SHARDSIZE) {
			flushOps();
		}
	}
}
