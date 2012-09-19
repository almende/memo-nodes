package com.chap.memo.memoNodes.bus;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.chap.memo.memoNodes.bus.json.ImportArcOpDeserializer;
import com.chap.memo.memoNodes.bus.json.ImportNodeValueDeserializer;
import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.ArcOpBuffer;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.model.NodeValueBuffer;
import com.eaio.uuid.UUID;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

public class MemoWriteBus {
	private final static MemoWriteBus bus = new MemoWriteBus();
	static final ObjectMapper om = new ObjectMapper();
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
			ops.clear();
		}
		synchronized(values){
			values.nodes.clear();
		}
	}
	public static void emptyDB(){
		emptyDB(new String[]{ "NodeValueIndex", "ArcOpIndex", "NodeValueShard",
				"ArcOpShard"});
	}
	public static void emptyDB(String[] types) {
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
		System.out.println("Importing DB!");
		SimpleModule nvModule = new SimpleModule("NodeValueDeserializer", new Version(1, 0, 0, "alpha", null, null))
		   .addDeserializer(NodeValue.class, new ImportNodeValueDeserializer());
		om.registerModule(nvModule);
		SimpleModule arcModule = new SimpleModule("ArcOpDeserializer", new Version(1, 0, 0, "alpha", null, null))
		   .addDeserializer(ArcOp.class, new ImportArcOpDeserializer());
		om.registerModule(arcModule);
		BufferedInputStream bus = new  BufferedInputStream(in,15000);
		ZipInputStream zis = new ZipInputStream(bus);
		ZipEntry entry = null;
		try {
			while ((entry = zis.getNextEntry()) != null){
				if (entry.getName().equals("values.json")){
					MappingIterator<NodeValue> iter= om.readValues(new JsonFactory().createJsonParser(zis), NodeValue.class);
					while (iter.hasNext()){
						this.store(iter.next());
					}
				}
				if (entry.getName().equals("arcs.json")){
					MappingIterator<ArcOp> iter = om.readValues(new JsonFactory().createJsonParser(zis), ArcOp.class);
					while (iter.hasNext()){
						this.store(iter.next());
					}
				}
			}
		} catch (Exception e){
			e.printStackTrace();
			return;
		}
		System.out.println("Done importing DB!");
		
	}
	
	public void flush() {
		if (readBus == null){
			readBus = MemoReadBus.getBus();
		}
		ops.flush();
		values.flush();
		readBus.loadIndexes(false);
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
