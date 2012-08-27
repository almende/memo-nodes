package com.chap.memo.memoNodes.bus;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.ArcOpBuffer;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.model.NodeValueBuffer;
import com.chap.memo.memoNodes.storage.ArcOpIndex;
import com.chap.memo.memoNodes.storage.ArcOpShard;
import com.chap.memo.memoNodes.storage.MemoStorable;
import com.chap.memo.memoNodes.storage.NodeValueIndex;
import com.chap.memo.memoNodes.storage.NodeValueShard;
import com.eaio.uuid.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class MemoReadBus {
	static final ObjectMapper om = new ObjectMapper();
	LoadingCache<Key, NodeValueShard> NodeValueShards = CacheBuilder
			.newBuilder().maximumWeight(15000000).weigher(new Weigher<Key,NodeValueShard>(){
				public int weigh(Key key,NodeValueShard shard){
//					System.out.println("Shard is "+shard.getStoredSize()+" bytes");
					return shard.getStoredSize();
				}				
			}).build(
					new CacheLoader<Key, NodeValueShard>() {
						public NodeValueShard load(Key key){
//							System.out.println("Need to load nv shard! ("+key+")");
							NodeValueShard shard = (NodeValueShard) MemoStorable.load(key);
							return shard;
						}
					}
			);
	LoadingCache<Key, ArcOpShard> ArcOpShards = CacheBuilder
			.newBuilder().maximumWeight(15000000).weigher(new Weigher<Key,ArcOpShard>(){
				public int weigh(Key key,ArcOpShard shard){
//					System.out.println("Shard is "+shard.getStoredSize()+" bytes");
					return shard.getStoredSize();
				}				
			}).build(
					new CacheLoader<Key, ArcOpShard>(){
						public ArcOpShard load(Key key){
//							System.out.println("Need to load ops shard! ("+key+")");
							return (ArcOpShard) MemoStorable.load(key);
						}
					}		
			);

	public List<NodeValueIndex> NodeValueIndexes = new ArrayList<NodeValueIndex>(
			100);
	public List<ArcOpIndex> ArcOpIndexes = new ArrayList<ArcOpIndex>(100);

	DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
	MemcacheService memCache = MemcacheServiceFactory.getMemcacheService();;
	long lastValueChange = System.currentTimeMillis();
	long lastOpsChange = System.currentTimeMillis();
	long lastIndexesRun = 0;

	private final static MemoReadBus bus = new MemoReadBus();

	MemoReadBus() {
		loadIndexes(false);
	};

	public void exportDB(OutputStream out, boolean history) {
		// Select export (first all, later history)
		try {
			ZipOutputStream zos = new ZipOutputStream(out);
			zos.putNextEntry(new ZipEntry("values.json"));
			OutputStreamWriter writer = new OutputStreamWriter(zos);
			
//			ObjectOutputStream oos = new ObjectOutputStream(out);
			Query q = new Query("NodeValueShard");
			PreparedQuery prep = datastore.prepare(q);
			Iterator<Entity> iter = prep.asIterable().iterator();
			writer.append("[");
			boolean first = true;
			while (iter.hasNext()) {
				Entity ent = iter.next();
				NodeValueShard shard = (NodeValueShard) MemoStorable.load(ent);
				shard.initMultimaps();
				Iterator<NodeValue> inner = shard.nodes.values().iterator();
				while (inner.hasNext()) {
					if (!first) writer.append(",");
					first=false;
					NodeValue nv = inner.next();
					writer.append(om.writeValueAsString(nv));
				}
			}
			writer.append("]");
			writer.flush();
			
			zos.closeEntry();
			zos.putNextEntry(new ZipEntry("arcs.json"));
			
			first=true;
			writer.append("[");
			q = new Query("ArcOpShard");
			prep = datastore.prepare(q);
			iter = prep.asIterable().iterator();
			while (iter.hasNext()) {
				Entity ent = iter.next();
				ArcOpShard shard = (ArcOpShard) MemoStorable.load(ent);
				shard.initMultimaps();
				Iterator<ArcOp> inner = shard.parents.values().iterator();
				while (inner.hasNext()) {
					if (!first) writer.append(",");
					first=false;
					ArcOp ao = inner.next();
					writer.append(om.writeValueAsString(ao));
				}
			}
			writer.append("]");
			writer.flush();
			zos.closeEntry();
			zos.flush();
			zos.close();
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void delShard(NodeValueShard oldShard) {
		// synchronized (NodeValueShards) {
		NodeValueShards.invalidate(oldShard.getMyKey());
		// }
	}

	public void delShard(ArcOpShard oldShard) {
		// synchronized(ArcOpShards){
		ArcOpShards.invalidate(oldShard.getMyKey());
		// }
	}

	void loadIndexes(boolean clear) {
//		System.out.println("reloading indexes:" + clear);
//		long start = System.currentTimeMillis();

		synchronized (NodeValueIndexes) {
			synchronized (ArcOpIndexes) {
//				System.out.println("Started out with: nvi:"
//						+ NodeValueIndexes.size() + ";aoi:"
//						+ ArcOpIndexes.size());
				if (clear) {
					NodeValueIndexes.clear();
					NodeValueShards.invalidateAll();
					ArcOpIndexes.clear();
					ArcOpShards.invalidateAll();
					lastValueChange = System.currentTimeMillis();
					lastOpsChange = System.currentTimeMillis();
					lastIndexesRun = 0;
				} else {
					Object lastUpdate = memCache.get("memoNodes_lastUpdate");
					if (lastUpdate != null) {
						long lastUpdateTime = (Long) lastUpdate;
						if (lastUpdateTime <= lastIndexesRun) {
//							System.out
//									.println("No update of indexes necessary");
							return; // Nothing new to expect;
						}
					}
				}
				Query q = new Query("NodeValueIndex").addSort("timestamp",
						Query.SortDirection.DESCENDING);
				PreparedQuery NodeValueIndexQuery = datastore.prepare(q);
				QueryResultList<Entity> rl = NodeValueIndexQuery
						.asQueryResultList(withLimit(1000));
				if (rl.size() > 0) {
					NodeValueIndexes.clear();
					for (Entity ent : rl) {
						MemoStorable obj = MemoStorable.load(ent);
						if (obj != null) {
							NodeValueIndex index = (NodeValueIndex) obj;
							NodeValueIndexes.add(0, index);
						}
					}
					lastValueChange = System.currentTimeMillis();
				}
				Collections.sort(NodeValueIndexes, Collections.reverseOrder());

				q = new Query("ArcOpIndex").addSort("timestamp");
				PreparedQuery ArcOpIndexQuery = datastore.prepare(q);
				rl = ArcOpIndexQuery.asQueryResultList(withLimit(1000));
				if (rl.size() > 0) {
					ArcOpIndexes.clear();
					for (Entity ent : rl) {
						ArcOpIndex index = (ArcOpIndex) MemoStorable.load(ent);
						ArcOpIndexes.add(index);
					}
					lastOpsChange = System.currentTimeMillis();
				}
				lastIndexesRun = System.currentTimeMillis();

//				System.out.println("Done reloading indexes:"
//						+ (lastIndexesRun - start) + "ms nvi:"
//						+ NodeValueIndexes.size() + ";aoi:"
//						+ ArcOpIndexes.size());
			}
		}
	}

	public void compactDB() {
		mergeNodeValueShards();
		mergeArcOpShards();
		memCache.put("memoNodes_lastUpdate", System.currentTimeMillis());
		loadIndexes(true);
	}

	public synchronized void mergeNodeValueShards() {
		System.out.println("mergeNodeValueShards called!");
		Query q = new Query("NodeValueShard").addSort("size",
				Query.SortDirection.ASCENDING);
		ArrayList<NodeValueShard> list = new ArrayList<NodeValueShard>(500);
		int size = 0;
		PreparedQuery shardsQuery = datastore.prepare(q);
		Iterator<Entity> iter = shardsQuery.asIterable(withLimit(500))
				.iterator();
		while (iter.hasNext()) {
			Entity ent = iter.next();
			System.out.println("Checking entity: " + ent.getProperty("size")
					+ ":" + size);
			if (size + (Long) ent.getProperty("size") <= NodeValueBuffer.STORESIZE) {
				size += (Long) ent.getProperty("size");
				list.add((NodeValueShard) MemoStorable.load(ent));
			} else {
				break;
			}
		}
		if (list.size() > 1) {
			System.out.println("Merge " + list.size() + " NodeValueShards!");
			NodeValueShard shard = new NodeValueShard(
					list.toArray(new NodeValueShard[0]));
			NodeValueIndex index = new NodeValueIndex(shard);
			addNodeValueIndex(index, shard);
			for (NodeValueShard other : list) {
				NodeValueIndex idx = removeNodeValueIndexByShard(other
						.getMyKey());
				if (idx != null)
					idx.delete();
				delShard(other);
				other.delete();
			}
		}
	}

	public synchronized void mergeArcOpShards() {
		System.out.println("mergeArcOpShards called!");
		Query q = new Query("ArcOpShard").addSort("size",
				Query.SortDirection.ASCENDING);
		ArrayList<ArcOpShard> list = new ArrayList<ArcOpShard>(100);
		int size = 0;
		PreparedQuery shardsQuery = datastore.prepare(q);
		Iterator<Entity> iter = shardsQuery.asIterable(withLimit(100))
				.iterator();
		while (iter.hasNext()) {
			Entity ent = iter.next();
			System.out.println("Checking entity: " + ent.getProperty("size")
					+ ":" + size);
			if (size + (Long) ent.getProperty("size") <= ArcOpBuffer.STORESIZE) {
				size += (Long) ent.getProperty("size");
				list.add((ArcOpShard) MemoStorable.load(ent));
			} else {
				break;
			}
		}
		if (list.size() > 1) {
			System.out.println("Merge " + list.size() + " ArcOpShards!");
			ArcOpShard shard = new ArcOpShard(list.toArray(new ArcOpShard[0]));
			ArcOpIndex index = new ArcOpIndex(shard);
			addOpsIndex(index, shard);
			for (ArcOpShard other : list) {
				ArcOpIndex idx = removeArcOpIndexByShard(other.getMyKey());
				if (idx != null)
					idx.delete();
				delShard(other);
				other.delete();
			}
		}
	}

	public void addNodeValueIndex(NodeValueIndex index, NodeValueShard shard) {
		// synchronized (NodeValueShards) {
		NodeValueShards.put(index.getShardKey(), shard);
		// }
		synchronized (NodeValueIndexes) {
			NodeValueIndexes.add(0, index);
			Collections.sort(NodeValueIndexes, Collections.reverseOrder());
		}
		lastValueChange = System.currentTimeMillis();
		memCache.put("memoNodes_lastUpdate", lastValueChange);
	}

	public void addOpsIndex(ArcOpIndex index, ArcOpShard shard) {
		// synchronized(ArcOpShards){
		ArcOpShards.put(index.getShardKey(), shard);
		// }
		synchronized (ArcOpIndexes) {
			ArcOpIndexes.add(index);
		}
		lastOpsChange = System.currentTimeMillis();
		memCache.put("memoNodes_lastUpdate", lastOpsChange);
	}

	public static MemoReadBus getBus() {
		return bus;
	}

	public boolean valueChanged(long timestamp) {
		return timestamp <= lastValueChange;
	}

	public boolean opsChanged(long timestamp) {
		return timestamp <= lastOpsChange;
	}

	public NodeValueShard getSparseNodeValueShard(long room) {
		NodeValueShard result = null;
		Iterator<NodeValueShard> iter;
		// synchronized(NodeValueShards){
		iter = ImmutableMap.copyOf(NodeValueShards.asMap()).values().iterator();
		// }
		while (iter.hasNext()) {
			NodeValueShard shard = iter.next();
			if (shard.isDeleted())
				continue;
			if (shard.getStoredSize() < NodeValueBuffer.STORESIZE - room ){
				result = shard;
				break;
			}
		}
		return result;
	}

	public ArcOpShard getSparseArcOpShard(long room) {
		ArcOpShard result = null;
		Iterator<ArcOpShard> iter;
		// synchronized(ArcOpShards){
		iter = ImmutableMap.copyOf(ArcOpShards.asMap()).values().iterator();
		// }
		while (iter.hasNext()) {
			ArcOpShard shard = iter.next();
			if (shard.isDeleted())
				continue;
			if (shard.getStoredSize() < ArcOpBuffer.STORESIZE - room ){
				result = shard;
				break;
			}
		}
		return result;
	}

	public NodeValueIndex removeNodeValueIndexByShard(Key shardKey) {
		synchronized (NodeValueIndexes) {
			Iterator<NodeValueIndex> iter = NodeValueIndexes.iterator();
			while (iter.hasNext()) {
				NodeValueIndex index = iter.next();
				if (index.getShardKey().equals(shardKey)) {
					iter.remove();
					return index;
				}
			}
		}
		return null;
	}

	public ArcOpIndex removeArcOpIndexByShard(Key shardKey) {
		synchronized (ArcOpIndexes) {
			Iterator<ArcOpIndex> iter = ArcOpIndexes.iterator();
			while (iter.hasNext()) {
				ArcOpIndex index = iter.next();
				if (index.getShardKey().equals(shardKey)) {
					iter.remove();
					System.out.println("Removed index from ArcOpIndexes:"
							+ ArcOpIndexes.size());
					return index;
				}
			}
		}
		return null;
	}

	public MemoNode find(UUID uuid) {
		NodeValue value = getValue(uuid);
		if (value != null) {
			return new MemoNode(value);
		}
		return null;
	}

	public MemoNode find(UUID uuid, long timestamp) {
		NodeValue value = getValue(uuid, timestamp);
		if (value != null) {
			return new MemoNode(value);
		}
		return null;
	}

	public ArrayList<MemoNode> findAll(UUID uuid) {
		return findAll(uuid, 0);
	}

	public ArrayList<MemoNode> findAll(UUID uuid, int retryCount) {
		ArrayList<MemoNode> result = new ArrayList<MemoNode>(100);

		ImmutableList<NodeValueIndex> copy;
		synchronized (NodeValueIndexes) {
			if (NodeValueIndexes.size() == 0)
				return result;
			// Iterator through indexes, copy because stuff might disappear
			copy = ImmutableList.copyOf(NodeValueIndexes);
		}
		Iterator<NodeValueIndex> iter = copy.iterator();
		while (iter.hasNext()) {
			NodeValueIndex index = iter.next();
			if (index.contains(uuid)) {
				NodeValueShard shard=null;
				try {
					shard = NodeValueShards.getUnchecked(index.getShardKey());
				} catch (Exception e){
					System.err.println("missed a ops shard, merged db? "+index.getShardKey());
				}
				if (shard == null) {
					// Happens if shard has been deleted, indication that
					// indexes are outdated!
					if (retryCount > 10) {
						System.err
								.println("Warning, even after 10 retries no shard found!");
						index.delete(true);
					}

					loadIndexes(true);
					return findAll(uuid, retryCount + 1);
				}
				if (shard != null) {
					for (NodeValue nv : shard.findAll(uuid)) {
						result.add(new MemoNode(nv));
					}
				}
			}
		}
		Collections.sort(result);
		return result;
	}

	public NodeValue getValue(UUID uuid) {
		return getValue(uuid, new Date().getTime());
	}

	public NodeValue getValue(UUID uuid, long timestamp) {
		return getValue(uuid, timestamp, 0);
	}

	public NodeValue getValue(UUID uuid, long timestamp, int retryCount) {
		NodeValue result = null;

		MemoWriteBus writeBus = MemoWriteBus.getBus();
		result = writeBus.values.findBefore(uuid, timestamp);
		ImmutableList<NodeValueIndex> copy;
		synchronized (NodeValueIndexes) {
			if (NodeValueIndexes.size() == 0) {
				return result;
			}
			copy = ImmutableList.copyOf(NodeValueIndexes);
		}
		Iterator<NodeValueIndex> iter = copy.iterator();
		while (iter.hasNext()) {
			NodeValueIndex index = iter.next();
			if (result != null
					&& index.getNewest() < result.getTimestamp_long()) {
				return result;
			}
			if (index.getOldest() < timestamp
					&& index.contains(uuid)) {
				NodeValueShard shard=null;
				try {
					shard = NodeValueShards.getUnchecked(index.getShardKey());
				} catch (Exception e){
					System.err.println("missed a ops shard, merged db? "+index.getShardKey());
				}
				if (shard == null) {
					// Happens if shard has been deleted, indication
					// that indexes are outdated!
					if (retryCount > 10) {
						System.err
								.println("Warning, even after 10 retries still no shard found!."
										+ uuid);
						index.delete(true);
					}
					// System.out.println("retry loadIndexes for value shard."+uuid);
					loadIndexes(true);
					return getValue(uuid, timestamp, retryCount + 1);
				}
				if (shard != null) {
					NodeValue res = shard.findBefore(uuid, timestamp);
					if (res == null) {
						// System.out.println("Value wasn't in this shard, trying next."+uuid);
						continue;
					}
					if (result == null
							|| res.getTimestamp_long() > result
									.getTimestamp_long()) {
						result = res;
					}
				}
			}
		}
		// System.out.println("Normal return:"+uuid+":"+new
		// String(result.getValue()));
		return result;
	}

	public ArrayList<ArcOp> getOps(UUID uuid, int type, long since) {
		return getOps(uuid, type, System.currentTimeMillis(), since);
	}

	public ArrayList<ArcOp> getOps(UUID uuid, int type, long timestamp,
			long since) {
		return getOps(uuid, type, timestamp, since, 0);
	}

	public ArrayList<ArcOp> getOps(UUID uuid, int type, long timestamp,
			long since, int retryCount) {
		ArrayList<ArcOp> wbresult = new ArrayList<ArcOp>(10);
		ArrayList<ArcOp> result = new ArrayList<ArcOp>(10);
		boolean doIndexes = false;
		ImmutableList<ArcOpIndex> copy = null;

		switch (type) {
		case 0: // parentList, UUID is child
			List<ArcOp> children = MemoWriteBus.getBus().ops.getChildOps(uuid);
			wbresult.ensureCapacity(wbresult.size() + children.size() + 1);
			for (ArcOp op : children) {
				if (op.getTimestamp_long() <= timestamp
						&& op.getTimestamp_long() >= since) {
					wbresult.add(op);
				}
			}
			break;
		case 1:
			List<ArcOp> parents = MemoWriteBus.getBus().ops.getParentOps(uuid);
			wbresult.ensureCapacity(wbresult.size() + parents.size() + 1);
			for (ArcOp op : parents) {
				if (op.getTimestamp_long() <= timestamp
						&& op.getTimestamp_long() >= since) {
					wbresult.add(op);
				}
			}
			break;
		}
		synchronized (ArcOpIndexes) {
			if (ArcOpIndexes.size() > 0
					&& ArcOpIndexes.get(ArcOpIndexes.size() - 1).getStoreTime() >= since) {
				doIndexes = true;
				copy = ImmutableList.copyOf(ArcOpIndexes);
			}
		}
		if (doIndexes) {
			Iterator<ArcOpIndex> iter = copy.iterator();
			while (iter.hasNext()) {
				ArcOpIndex index = iter.next();
				if (index.getStoreTime() < since) {
					continue;
				}
				switch (type) {
				case 0: // parentList, UUID is child
					if (index.containsChild(uuid)) {
						ArcOpShard shard=null;
						try {
							shard = ArcOpShards.getUnchecked(index.getShardKey());
						} catch (Exception e){
							System.err.println("missed a ops shard, merged db? "+index.getShardKey());
						}
						if (shard == null) {
							// Happens if shard has been deleted, indication
							// that indexes are outdated!
							if (retryCount > 10) {
								System.err
										.println("Warning, even after 10 retries still no shard found!"
												+ uuid);
								index.delete(true);
							}
							loadIndexes(true);
							System.err.println("Doing retry" + retryCount
									+ " uuid" + uuid);
							return getOps(uuid, type, timestamp, since,
									retryCount + 1);
						}
						if (shard != null) {
							List<ArcOp> children = shard.getChildOps(uuid);
							result.ensureCapacity(children.size() + 1);
							for (ArcOp op : children) {
								if (op.getTimestamp_long() <= timestamp) {
									result.add(op);
								}
							}
						}
					}
					break;
				case 1: // parentList, UUID is child
					if (index.containsParent(uuid)) {
						ArcOpShard shard=null;
						try {
							shard = ArcOpShards.getUnchecked(index.getShardKey());
						} catch (Exception e){
							System.err.println("missed a ops shard, merged db? "+index.getShardKey());
						}
						if (shard == null) {
							// Happens if shard has been deleted, indication
							// that indexes are outdated!
							if (retryCount > 10) {
								System.err
										.println("Warning, even after 10 retries still no shard found!");
								index.delete(true);
							}
							loadIndexes(true);
							System.err.println("Doing retry" + retryCount
									+ " uuid:" + uuid);
							return getOps(uuid, type, timestamp, since,
									retryCount + 1);
						}
						if (shard != null) {
							List<ArcOp> parents = shard.getParentOps(uuid);
							result.ensureCapacity(parents.size() + 1);
							for (ArcOp op : parents) {
								if (op.getTimestamp_long() <= timestamp) {
									result.add(op);
								}
							}
						}
					}
					break;
				}
			}
		}
		result.addAll(wbresult);
		return result;
	}

}
