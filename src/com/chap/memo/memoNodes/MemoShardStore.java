package com.chap.memo.memoNodes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;

public class MemoShardStore {
	// These can be tweaked to modify performance and memory usage of the
	// application:
	protected static final int SHARDSIZE = 40000; // How many entries does a
													// shard have
	protected static final int NOFSHARDS = 20; // How many shards do we keep in
												// cache
	protected static final int NOFINDEXES = 25; // How many entries does a
												// MemoGroupIndex hold

	protected static UUID INSTANCEID = new UUID();

	static DatastoreService datastore = DatastoreServiceFactory
			.getDatastoreService();
	static MemoShard currentShard = new MemoShard();

	static MemoGroupIndex rootIndex = new MemoGroupIndex();
	static Map<Key, MemoShard> shards = Collections
			.synchronizedMap(new MyMap<Key, MemoShard>(NOFSHARDS, new Float(
					0.5), true));
	
	static public void flush(){
		storeShard(currentShard);
	}
	
	static protected void storeShard(MemoShard shard) {
		// What if stored before? Get old key from index! Happens on multiple flushes for the currentShard.
		Entity shardEntity;
		boolean storedBefore= false;
		if (shard.index.shardKey != null) {
			shardEntity = new Entity(shard.index.shardKey);
			storedBefore= true;
		} else {
			shardEntity = new Entity("MemoShardData");
		}
		byte[] data = shard.data.serialize();
		// TODO: What to do if size is too big? Split in multiple Properties?
		// System.out.println("Storing: "+data.length + " bytes in shard.");
		shardEntity.setProperty("shard", new Blob(data));
		shardEntity.setProperty("size", data.length);
		datastore.put(shardEntity);
		Key shardKey = shardEntity.getKey();

		Entity shardIndex;
		if (shard.index.myKey != null) {
			shardIndex = new Entity(shard.index.myKey);
		} else {
			shardIndex = new Entity("MemoShardIndex", shardKey);
		}
		shard.index.shardKey = shardKey;
		shardIndex.setProperty("index", new Blob(shard.index.serialize()));
		datastore.put(shardIndex);
		shard.index.myKey = shardIndex.getKey();
		if (!storedBefore){
			rootIndex = rootIndex.addIndex(shard.index);
			storeRootIndex();//Required, or else we miss on newest shards
		}
	}

	static protected void storeIndex(MemoIndex index) {
		Entity shardIndex;
		if (index.myKey != null) {
			shardIndex = new Entity(index.myKey);
		} else {
			shardIndex = new Entity("MemoIndex");
		}
		shardIndex.setProperty("index", new Blob(index.serialize()));
		datastore.put(shardIndex);
		index.myKey = shardIndex.getKey();
		//System.out.println("Stored index:"+index.myKey);
	}

	static protected MemoIndex loadIndex(Key key) {
		try {
			Blob blob = (Blob) datastore.get(key).getProperty("index");
			byte[] data = blob.getBytes();
			MemoIndex res = MemoIndex.unserialize(data);
			res.myKey = key;
			//System.out.println("loaded index:"+res.type+":"+res.myKey);
			return res;
		} catch (EntityNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	static protected void storeRootIndex() {
		Entity shardIndex;
		if (rootIndex.myKey != null) {
			shardIndex = new Entity(rootIndex.myKey);
		} else {
			shardIndex = new Entity("rootIndex");
		}
		shardIndex.setProperty("index", new Blob(rootIndex.serialize()));

		shardIndex.setProperty("InstanceId", INSTANCEID.toString());
		datastore.put(shardIndex);
		rootIndex.myKey = shardIndex.getKey();
		//System.out.println("Stored rootindex:"+rootIndex.myKey + " with "+rootIndex.subindexes.size() + " elements");
	}

	static protected ArrayList<MemoIndex> loadRootIndexes() {
		Query q = new Query("rootIndex");
		PreparedQuery pq = datastore.prepare(q);
		ArrayList<MemoIndex> result = new ArrayList<MemoIndex>(pq.countEntities());
		ArrayList<Key> toDelete = new ArrayList<Key>(1);
		
		Iterator<Entity> iter = pq.asIterator();
		while (iter.hasNext()) {
			Entity ent = iter.next();
			byte[] data = ((Blob) ent.getProperty("index")).getBytes();
			MemoIndex idx = MemoIndex.unserialize(data);
			if (idx == null || idx.firstTime == null){
				System.out.println("Warning: invalid index found! (Deleting it from the datastore)");
				toDelete.add(ent.getKey());
				continue;
			}
			idx.myKey = ent.getKey();
			result.add(idx);
		}
		datastore.delete(toDelete);
		Collections.sort(result);
		/*
		for (MemoIndex idx: result){
			System.out.println("loaded rootIndex:"+idx.type+":"+idx.myKey);
		}
		*/
		return result;
	}

	static protected MemoShardData loadShardData(Key key) {
		if (key == null) {
			System.out.println("Error: Shard with Null key requested!");
			return null;
		}
		try {
			Entity shardData=datastore.get(key);
			Long size = (Long) shardData.getProperty("size");
			byte[] data = new byte[size.intValue()];
			data = ((Blob) shardData.getProperty("shard"))
					.getBytes();
			// System.out.println("Loading: "+data.length +
			// " bytes from shard.");
			MemoShardData res = MemoShardData.unserialize(data);
			//System.out.println("loaded loadShardData:"+res.toString()+":"+res.nodes.size());
			return res;
		} catch (EntityNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	static protected Node findNewest(ArrayList<IndexKey> list, UUID nodeId,
			Date timestamp, Date before) {
		Iterator<IndexKey> iter = list.iterator();
		//System.out.println("Searching through "+list.size()+" items");
		//int count=0;
		while (iter.hasNext()) {
			//count++;
			IndexKey idxKey = iter.next();
			if (idxKey.timestamp.before(timestamp)) {
				//System.out.println("time:"+count+"/"+list.size());
				return null;
			}
			MemoIndex inner = MemoShardStore.loadIndex(idxKey.key);
			if (before != null && inner.lastTime.after(before)) {
				//System.out.println("tooNew:"+count+"/"+list.size());
				return null;
			}
			if (inner.type.equals("MemoGroupIndex")) {
				MemoGroupIndex index = (MemoGroupIndex) inner;
				//System.out.println("recurse:"+count+"/"+list.size());
				Node res = findNewest(index.subindexes, nodeId, timestamp,
						before);
				if (res != null) {
					return res;
				}
			}
			if (inner.type.equals("ShardIndex")) {
				MemoShardIndex index = (MemoShardIndex) inner;
				if (index.nodeids.contains(nodeId)) {
					MemoShardData data = MemoShardStore
							.loadShardData(index.shardKey);
					Node result = data.find(nodeId);
					Date nodeTime = result.getTimestamp();
					if (nodeTime.after(timestamp)) {
						if (before == null || nodeTime.before(before) || nodeTime.equals(before)) {
							MemoShard shard = new MemoShard();
							shard.index = index;
							shard.data = data;
							shards.put(idxKey.key, shard);
							//System.out.println("load:"+count+"/"+list.size());
							return result;
						}
					}
				} else {
					//System.out.println("Not in "+inner.myKey);
				}
			}
		}
		//System.out.println("notFound:"+count+"/"+list.size());
		return null;
	}

	static protected Node loadNode(UUID nodeId, Date before) {
		MemoShard shard = currentShard;
		// First check currentShard
		if (shard.index.nodeids.contains(nodeId)) {
			Node result = shard.data.find(nodeId);
			if (before == null || result.getTimestamp().before(before)) {
				//System.out.print("(current)");
				return result;
			}
		}
		// Search in-memory cached shards
		synchronized (shards) {
			for (MemoShard next : shards.values()) {
				shard = next;
				if (shard.index.nodeids.contains(nodeId)) {
					Node result = shard.data.find(nodeId);
					if (before == null
						|| result.getTimestamp().before(before) || result.getTimestamp().equals(before)) {
						//System.out.print("(shards)");
						return result;
					}
				}
			}
		}
		ArrayList<MemoIndex> rootIndexes = MemoShardStore.loadRootIndexes();
		Node result = null;
		Date timestamp = new Date(0);
		for (MemoIndex idx : rootIndexes) { // time ordered list;
			if (idx.firstTime.after(timestamp)
					&& (before == null || idx.lastTime.before(before))) {
				MemoGroupIndex index = (MemoGroupIndex) idx;
				result = findNewest(index.subindexes, nodeId, timestamp, before);
				if (result != null)
					timestamp = result.getTimestamp();
			} else {
				break;
			}
		}
		if (result != null) {
			//System.out.print("(loaded)");
			return result;
		}
		System.out.println("Couldn't find node:"+nodeId);
		return null;
	}

	/* Published API */
	static public void addNode(Node node) {
		currentShard.addNode(node);
	}

	static public Node findNode(UUID nodeId) {
		Node result = loadNode(nodeId, null);
		return result;
	}

	static public Node findBefore(UUID nodeId, Date timestamp) {
		Node result = loadNode(nodeId, timestamp);
		return result;
	}

	static public ArrayList<Node> findAll(UUID nodeId) {
		System.out.println("NOT IMPLEMENTED YET! FindAll called " + nodeId);
		// TODO: still needs to be implemented
		return new ArrayList<Node>();
	}

	static public void emptyDB() {
		// create one big cleanup query
		String[] types = { "rootIndex", "MemoShardIndex", "MemoShardData",
				"MemoGroupIndex", "MemoIndex" };
		for (String type : types) {
			Query q = new Query(type).setKeysOnly();
			PreparedQuery pq = datastore.prepare(q);
			// int count = pq.countEntities();
			// System.out.println("Deleting :"+count+" entries of type:"+type);
			for (Entity res : pq.asIterable()) {
				datastore.delete(res.getKey());
			}
		}
		
		// Clean out memory, can only cleanup the ones I have reference to.
		INSTANCEID = new UUID();
		rootIndex = new MemoGroupIndex();
		shards.clear();
		currentShard = new MemoShard();
		System.out.println("Database cleared!");
	}
}

@SuppressWarnings("rawtypes")
class MyMap<K, V> extends LinkedHashMap<K, V> {
	private static final long serialVersionUID = 1L;

	MyMap(int capacity, float loadFactor, boolean order) {
		super(capacity, loadFactor, order);
	}

	protected boolean removeEldestEntry(Map.Entry eldest) {
		synchronized (this) {
			if (size() > MemoShardStore.NOFSHARDS) {
				this.remove(eldest.getKey());
			}
		}
		return false;
	}
}

class MemoShard {
	MemoShardData data = new MemoShardData();
	MemoShardIndex index = new MemoShardIndex();
	
	public synchronized void addNode(Node node) {
		this.data.store(node);
		this.index.addNode(node);
		if (this.data.knownNodes.size() >= MemoShardStore.SHARDSIZE) {
			//System.out.println("New currentshard");
			MemoShardStore.storeShard(this);
			//MemoShardStore.shards.put(this.index.myKey, this);
			MemoShardStore.shards.put(this.index.myKey, this);
			//System.out.println("node "+node.getValue()+" stored in shard:"+this.index.myKey);
			MemoShardStore.currentShard = new MemoShard();
		}
	}
}

class MemoStorable implements Serializable {
	private static final long serialVersionUID = -5770613002073776843L;

	public byte[] serialize() {
		byte[] result = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ZipOutputStream zos = new ZipOutputStream(bos);
		try {
			//zos.setLevel(4);
			zos.putNextEntry(new ZipEntry("Object"));
			ObjectOutputStream oos = new ObjectOutputStream(zos);
			//ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(this);
			oos.flush();
			oos.reset();
			zos.closeEntry();
			zos.flush();
			result = bos.toByteArray();
			bos.reset();
			bos.close();
			zos.close();
			oos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static MemoStorable _unserialize(byte[] data) {
		MemoStorable result = null;
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		ZipInputStream zis = new ZipInputStream(bis);
		try {
			zis.getNextEntry();
			ObjectInputStream ios = new ObjectInputStream(zis);
			//ObjectInputStream ios = new ObjectInputStream(bis);
			result = (MemoStorable) ios.readObject();
			zis.closeEntry();
			bis.reset();
			bis.close();
			zis.close();
			ios.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return result;
	}
}

class MemoShardData extends MemoStorable {
	private static final long serialVersionUID = -5770613002073776843L;
	HashMap<UUID, ArrayList<Node>> knownNodes = new HashMap<UUID, ArrayList<Node>>(MemoShardStore.SHARDSIZE);

	public static MemoShardData unserialize(byte[] data) {
		return (MemoShardData) _unserialize(data);
	}
	
	public Node find(UUID id) {
		ArrayList<Node> res = knownNodes.get(id);
		if (res != null && !res.isEmpty()) {
			Node result = null;
			Iterator<Node> iter = res.iterator();
			while (iter.hasNext()) {
				Node next = iter.next();
				if (result == null
						|| next.getTimestamp().after(result.getTimestamp())) {
					result = next;
				}
			}
			return result;
		}
		return null;
	}

	public Node findBefore(UUID id, Date timestamp) {
		ArrayList<Node> res = knownNodes.get(id);
		if (res != null && !res.isEmpty()) {
			Node result = null;
			Iterator<Node> iter = res.iterator();
			while (iter.hasNext()) {
				Node next = iter.next();
				if (next.getTimestamp().before(timestamp)) {
					if (result == null
							|| next.getTimestamp().after(result.getTimestamp())) {
						result = next;
					}
				}
			}
			return result;
		}
		return null;
	}

	public void store(Node node) {
		ArrayList<Node> cur = knownNodes.get(node.getId());
		if (cur != null) {
			int size = cur.size();
			boolean found = false;
			for (int i = 0; i < size; i++) {
				Date comp = cur.get(i).getTimestamp();
				if (node.getTimestamp().equals(comp)
						|| comp.before(node.getTimestamp())) {
					cur.add(i, node);
					found = true;
					break;
				}
			}
			if (!found) {
				cur.add(node);
			}
		} else {
			cur = new ArrayList<Node>(3);
			cur.add(node);
		}
		knownNodes.put(node.getId(), cur);
	}

	public ArrayList<Node> findAll(UUID id) {
		return knownNodes.get(id);
	}
	
}

class MemoIndex extends MemoStorable implements Comparable<MemoIndex> {
	private static final long serialVersionUID = 8232550041630847696L;
	Key myKey = null;
	Date firstTime = null;
	Date lastTime = null;
	String type = "generic";

	public static MemoIndex unserialize(byte[] data) {
		MemoIndex index = (MemoIndex) _unserialize(data);
		if (index.type.equals("ShardIndex"))
			return (MemoShardIndex) index;
		if (index.type.equals("ShardGroupIndex"))
			return (MemoGroupIndex) index;
		return index;
	}

	@Override
	public int compareTo(MemoIndex o) {
		if (this.firstTime == null || o.firstTime == null){
			System.out.println("Warning, invalid index found:"+this.myKey+","+this.firstTime+" - "+o.myKey+","+o.firstTime);
			return -1;
		}
		return -1 * (this.firstTime.compareTo(o.firstTime));
	}
}

class MemoGroupIndex extends MemoIndex {
	private static final long serialVersionUID = 2550902839122518068L;
	ArrayList<IndexKey> subindexes = new ArrayList<IndexKey>(
			MemoShardStore.NOFINDEXES);

	public MemoGroupIndex() {
		this.type = "MemoGroupIndex";
	}

	public MemoGroupIndex addIndex(MemoIndex index) {
		if (firstTime == null || firstTime.after(index.firstTime)) {
			firstTime = index.firstTime;
		}
		if (lastTime == null || lastTime.before(index.lastTime)) {
			lastTime = index.lastTime;
		}
		boolean found = false;
		for (int i = 0; i < this.subindexes.size(); i++) {
			if (this.subindexes.get(i).timestamp.after(index.firstTime)) {
				this.subindexes.add(i, new IndexKey(index.myKey,
						index.firstTime));
				found = true;
				break;
			}
		}
		if (!found)
			this.subindexes.add(new IndexKey(index.myKey, index.firstTime));
		if (this.subindexes.size() >= MemoShardStore.NOFINDEXES) {
			MemoGroupIndex newIdx = new MemoGroupIndex();
			newIdx.myKey = this.myKey;
			this.myKey = null;
			MemoShardStore.storeIndex(this);
			//System.out.println("Stored groupIdx with "+this.subindexes.size() + " elements");
			newIdx.addIndex(this);
			return newIdx;
		}
		//System.out.println("added index to groupIdx:"+this.subindexes.size());
		return this;
	}

	public static MemoGroupIndex unserialize(byte[] data) {
		return (MemoGroupIndex) _unserialize(data);
	}
}

class MemoShardIndex extends MemoIndex {
	private static final long serialVersionUID = -5770613002073776843L;
	HashSet<UUID> nodeids = new HashSet<UUID>(MemoShardStore.SHARDSIZE);
	Key shardKey = null;

	public MemoShardIndex() {
		this.type = "ShardIndex";
	}

	public void addNode(Node node) {
		this.nodeids.add(node.getId());
		Date nodeTime = node.getTimestamp();
		if (firstTime == null || firstTime.before(nodeTime)) {
			firstTime = nodeTime;
		}
		if (lastTime == null || lastTime.after(nodeTime)) {
			lastTime = nodeTime;
		}
	}

	public static MemoShardIndex unserialize(byte[] data) {
		return (MemoShardIndex) _unserialize(data);
	}
}

class IndexKey implements Serializable {
	private static final long serialVersionUID = -4804333997966989141L;
	Key key = null;
	Date timestamp = null;

	public IndexKey(Key key, Date timestamp) {
		this.key = key;
		this.timestamp = timestamp;
	}
}
