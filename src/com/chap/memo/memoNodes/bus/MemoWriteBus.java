package com.chap.memo.memoNodes.bus;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

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
		MemoReadBus.getBus().loadIndexes(true);
		System.out.println("Database cleared!");
	}

	public void importDB(InputStream in){
		BufferedInputStream bus = new  BufferedInputStream(in,15000);
		ObjectInputStream ios;
		try {
			ios = new ObjectInputStream(bus);
			while (true){
				Object elem = ios.readObject();
				if (elem == null) break;
				if (elem instanceof ArcOp) store((ArcOp)elem);
				if (elem instanceof NodeValue) store((NodeValue)elem);
			}
		} catch (EOFException eof){
			System.out.println("Done importDB...");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void flush() {
		if (readBus == null){
			readBus = MemoReadBus.getBus();
		}
		ops.flush();
		values.flush();
		readBus.loadIndexes(true);
	}

	public NodeValue store(NodeValue result){
		if (readBus == null){
			readBus = MemoReadBus.getBus();
		}
		values.store(result);
		readBus.lastValueChange = System.currentTimeMillis();
		return result;
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
