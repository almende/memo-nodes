package com.chap.memo.memoNodes.bus;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.MemoUtils;
import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.ArcOpBuffer;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.model.NodeValueBuffer;
import com.chap.memo.memoNodes.model.OpsType;
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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class MemoReadBus {
	static final ObjectMapper om = new ObjectMapper();
	long maxWeight = Runtime.getRuntime().maxMemory() / 4;
	
	LoadingCache<Key, NodeValueShard> NodeValueShards = CacheBuilder
			
			.newBuilder().maximumWeight(maxWeight)
//			.concurrencyLevel(1)
			.weigher(new Weigher<Key, NodeValueShard>() {
				public int weigh(Key key, NodeValueShard shard) {
//					System.out.println("NodeValue Shard is "+shard.getSize()+"/"+shard.getStoredSize()+" bytes ("+(shard.getStoredSize()*100)/shard.getSize()+"%) into cache:"+NodeValueShards.size());
					return shard.getSize()*2; 
				}
			}).build(new CacheLoader<Key, NodeValueShard>() {
				public NodeValueShard load(Key key) {
					// System.out.println("Need to load nv shard! ("+key+")");
					NodeValueShard shard = (NodeValueShard) MemoStorable
							.load(key);
					return shard;
				}
			});
	LoadingCache<Key, ArcOpShard> ArcOpShards = CacheBuilder.newBuilder()
//			.concurrencyLevel(1)
			.maximumWeight(maxWeight)
			.weigher(new Weigher<Key, ArcOpShard>() {
				public int weigh(Key key, ArcOpShard shard) {
//					System.out.println("ArcOp Shard is "+shard.getSize()+"/"+shard.getStoredSize()+" bytes ("+(shard.getStoredSize()*100)/shard.getSize()+"%) into cache:"+ArcOpShards.size());
					return shard.getSize()*2;
				}
			}).build(new CacheLoader<Key, ArcOpShard>() {
				public ArcOpShard load(Key key) {
					// System.out.println("Need to load ops shard! ("+key+")");
					return (ArcOpShard) MemoStorable.load(key);
				}
			});

	public List<NodeValueIndex> NodeValueIndexes = new ArrayList<NodeValueIndex>(100);
	public List<ArcOpIndex> ArcOpIndexes = new ArrayList<ArcOpIndex>(100);


	DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
	MemcacheService memCache = MemcacheServiceFactory.getMemcacheService();;
	long lastValueChange = System.currentTimeMillis();
	long lastOpsChange = System.currentTimeMillis();
	long lastIndexesRun = 0;

	private final static MemoReadBus bus = new MemoReadBus();

	MemoReadBus() {
		System.out.println("MaxWeight is:"+maxWeight);
		loadIndexes(false);
	};

//	public void dropHistory() {
//		synchronized (NodeValueIndexes) {
//			HashMap<UUID, NodeValue> newestNodeValues = new HashMap<UUID, NodeValue>(
//					100000);
//
//			// ObjectOutputStream oos = new ObjectOutputStream(out);
//			Query q = new Query("NodeValueShard");
//			PreparedQuery prep = datastore.prepare(q);
//			Iterator<Entity> iter = prep.asIterable().iterator();
//			while (iter.hasNext()) {
//				Entity ent = iter.next();
//				NodeValueShard shard = (NodeValueShard) MemoStorable.load(ent);
//				Iterator<NodeValue> inner = Arrays.asList(shard.getNodes()).iterator();
//				while (inner.hasNext()) {
//					NodeValue nv = inner.next();
//					if (newestNodeValues.containsKey(nv.getId())) {
//						NodeValue other = newestNodeValues.get(nv.getId());
//						if (other.compareTo(nv) < 0) { // other older
//														// than current?
//							newestNodeValues.put(nv.getId(), nv);
//							nv = other; // export other, i.s.o. current
//						}
//					} else {
//						newestNodeValues.put(nv.getId(), nv);
//					}
//				}
//			}
//
//			if (newestNodeValues.size() > 0) {
//				NodeValue[] list = newestNodeValues.values().toArray(
//						new NodeValue[0]);
//				Arrays.sort(list);
//
//				MemoWriteBus writeBus = MemoWriteBus.getBus();
//				MemoWriteBus.emptyDB(new String[] { "NodeValueIndex",
//						"NodeValueShard" });
//				for (NodeValue val : list) {
//					if (val.getValue().length >= 0) {
//						writeBus.store(val);
//					}
//				}
//				MemoNode.flushDB();
//				loadIndexes(true);
//			}
//		}
//		synchronized (ArcOpIndexes) {
//			ArrayListMultimap<Long, ArcOp> newestArcOps = ArrayListMultimap
//					.create();
//			Query q = new Query("ArcOpShard");
//			PreparedQuery prep = datastore.prepare(q);
//			Iterator<Entity> iter = prep.asIterable().iterator();
//			while (iter.hasNext()) {
//				Entity ent = iter.next();
//				ArcOpShard shard = (ArcOpShard) MemoStorable.load(ent);
//				Iterator<ArcOp> inner = Arrays.asList(shard.getOps()).iterator();
//				while (inner.hasNext()) {
//					ArcOp ao = inner.next();
//					List<ArcOp> res = newestArcOps.removeAll(ao
//							.getTimestamp_long());
//					if (res.size() > 0) {
//						List<ArcOp> newList = new ArrayList<ArcOp>(res.size());
//						for (ArcOp other : res) {
//							if (other.getChild().equals(ao.getChild())
//									&& other.getParent().equals(ao.getParent())) {
//								if (other.compareTo(ao) < 0) { // other older than current?
//									ArcOp tmp = other; // swap
//									other = ao;
//									ao = tmp;
//								}
//							}
//							newList.add(other);
//						}
//						newList.add(ao);
//						newestArcOps.putAll(ao.getTimestamp_long(), newList);
//					} else {
//						newestArcOps.put(ao.getTimestamp_long(), ao);
//					}
//				}
//			}
//			if (newestArcOps.size() > 0) {
//				ArcOp[] list = newestArcOps.values().toArray(new ArcOp[0]);
//				Arrays.sort(list);
//
//				MemoWriteBus writeBus = MemoWriteBus.getBus();
//				MemoWriteBus
//						.emptyDB(new String[] { "ArcOpIndex", "ArcOpShard" });
//				for (ArcOp val : list) {
//					if (val.getType().equals(OpsType.ADD)) {
//						writeBus.store(val);
//					}
//				}
//				MemoNode.flushDB();
//				loadIndexes(true);
//			}
//		}
//	}

	public void exportDB(OutputStream out, boolean history) {
		// Select export (first all, later history)
		try {
			ZipOutputStream zos = new ZipOutputStream(out);
			OutputStreamWriter writer = new OutputStreamWriter(zos);
			int count = 0;

			synchronized (NodeValueIndexes) {

				zos.putNextEntry(new ZipEntry("values.json"));
				HashMap<UUID, NodeValue> newestNodeValues = new HashMap<UUID, NodeValue>(
						100000);

				// ObjectOutputStream oos = new ObjectOutputStream(out);
				Query q = new Query("NodeValueShard").addSort("timestamp");
				PreparedQuery prep = datastore.prepare(q);
				Iterator<Entity> iter = prep.asIterable().iterator();
				writer.append("[");

				while (iter.hasNext()) {
					Entity ent = iter.next();
					NodeValueShard shard = (NodeValueShard) MemoStorable
							.load(ent);
					Iterator<NodeValue> inner = Arrays.asList(shard.getNodes()).iterator();
					while (inner.hasNext()) {
						boolean export = true;
						NodeValue nv = inner.next();
						if (history) {
							if (newestNodeValues.containsKey(nv.getId())) {
								NodeValue other = newestNodeValues.get(nv
										.getId());
								if (other.compareTo(nv) < 0) { // other older
																// than current?
									newestNodeValues.put(nv.getId(), nv);
									nv = other; // export other, i.s.o. current
								}
							} else {
								newestNodeValues.put(nv.getId(), nv);
								export = false;
							}
						}
						if (export) {
							if (count > 0)
								writer.append(",");
							count++;
							writer.append(om.writeValueAsString(nv));
						}
					}
				}

				if (history && newestNodeValues.size() > 0) {
					int oldcount = count;
					NodeValue[] list = newestNodeValues.values().toArray(
							new NodeValue[0]);
					Arrays.sort(list);

					MemoWriteBus writeBus = MemoWriteBus.getBus();
					MemoWriteBus.emptyDB(new String[] { "NodeValueIndex",
							"NodeValueShard" });
					for (NodeValue val : list) {
						if (val.getValue().length >= 0) {
							writeBus.store(val);
						} else {
							if (count > 0)
								writer.append(",");
							count++;
							writer.append(om.writeValueAsString(val));
						}
					}
					System.out.println("Kept:"
							+ (newestNodeValues.size() - (count - oldcount))
							+ " nodeValues");
					MemoNode.flushDB();
					loadIndexes(true);
				}
				writer.append("]");
				writer.flush();
				zos.closeEntry();
				System.out.println("Exported:" + count + " nodeValues");
			}
			synchronized (ArcOpIndexes) {
				zos.putNextEntry(new ZipEntry("arcs.json"));

				ArrayListMultimap<Long, ArcOp> newestArcOps = ArrayListMultimap
						.create();

				count = 0;
				writer.append("[");
				Query q = new Query("ArcOpShard").addSort("timestamp");;
				PreparedQuery prep = datastore.prepare(q);
				Iterator<Entity> iter = prep.asIterable().iterator();
				while (iter.hasNext()) {
					Entity ent = iter.next();
					ArcOpShard shard = (ArcOpShard) MemoStorable.load(ent);
					Iterator<ArcOp> inner = Arrays.asList(shard.getOps()).iterator();
					while (inner.hasNext()) {
						boolean export = true;
						ArcOp ao = inner.next();
						if (history) {
							List<ArcOp> res = newestArcOps.removeAll(ao
									.getTimestamp_long());
							if (res.size() > 0) {
								List<ArcOp> newList = new ArrayList<ArcOp>(
										res.size());
								boolean found = false;
								for (ArcOp other : res) {
									if (other.getChild().equals(ao.getChild())
											&& other.getParent().equals(
													ao.getParent())) {
										if (other.compareTo(ao) < 0) { // other
																		// older
																		// than
																		// current?
											ArcOp tmp = other; // swap
											other = ao;
											ao = tmp;
										}
										found = true;
									}
									newList.add(other);
								}
								if (!found) {
									export = false;
								}
								newList.add(ao);
								newestArcOps.putAll(ao.getTimestamp_long(),
										newList);
							} else {
								newestArcOps.put(ao.getTimestamp_long(), ao);
								export = false;
							}
						}
						if (export) {
							if (count > 0)
								writer.append(",");
							count++;
							writer.append(om.writeValueAsString(ao));
						}
					}
				}
				if (history && newestArcOps.size() > 0) {
					ArcOp[] list = newestArcOps.values().toArray(new ArcOp[0]);
					Arrays.sort(list);

					MemoWriteBus writeBus = MemoWriteBus.getBus();
					MemoWriteBus.emptyDB(new String[] { "ArcOpIndex",
							"ArcOpShard" });
					for (ArcOp val : list) {
						if (val.getType().equals(OpsType.ADD)) {
							writeBus.store(val);
						} else {
							if (count > 0)
								writer.append(",");
							count++;
							writer.append(om.writeValueAsString(val));
						}
					}
					System.out.println("Kept:" + newestArcOps.values().size()
							+ " ArcOps");
					MemoNode.flushDB();
					loadIndexes(true);

				}

				writer.append("]");
				writer.flush();
				zos.closeEntry();
				zos.flush();
				zos.close();
				out.flush();
				out.close();
				System.out.println("Exported:" + count + " ArcOps");
			}
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
		// long start = System.currentTimeMillis();

		synchronized (NodeValueIndexes) {
			synchronized (ArcOpIndexes) {
				// System.out.println("Started out with: nvi:"
				// + NodeValueIndexes.size() + ";aoi:"
				// + ArcOpIndexes.size());
				if (clear) {
					NodeValueShards.invalidateAll();
					ArcOpShards.invalidateAll();
					NodeValueIndexes.clear();
					ArcOpIndexes.clear();
					lastValueChange = System.currentTimeMillis();
					lastOpsChange = System.currentTimeMillis();
					lastIndexesRun = 0;
				} else {
					Object lastUpdate = memCache.get("memoNodes_lastUpdate");
					if (lastUpdate != null) {
						long lastUpdateTime = (Long) lastUpdate;
						if (lastUpdateTime <= lastIndexesRun) {
							// System.out
							// .println("No update of indexes necessary");
							return; // Nothing new to expect;
						}
					}
				}
				Query q = new Query("NodeValueIndex").addSort("timestamp",
						Query.SortDirection.DESCENDING);
				PreparedQuery NodeValueIndexQuery = datastore.prepare(q);
				QueryResultList<Entity> rl = NodeValueIndexQuery
						.asQueryResultList(withLimit(10000));
				if (rl.size() > 0) {
					//NodeValueShards.invalidateAll();
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

				q = new Query("ArcOpIndex").addSort("timestamp",
						Query.SortDirection.DESCENDING);
				PreparedQuery ArcOpIndexQuery = datastore.prepare(q);
				rl = ArcOpIndexQuery.asQueryResultList(withLimit(10000));
				if (rl.size() > 0) {
					//ArcOpShards.invalidateAll();
					ArcOpIndexes.clear();
					for (Entity ent : rl) {
						ArcOpIndex index = (ArcOpIndex) MemoStorable.load(ent);
						ArcOpIndexes.add(index);
					}
					lastOpsChange = System.currentTimeMillis();
				}
				lastIndexesRun = System.currentTimeMillis();

				// System.out.println("Done reloading indexes:"
				// + (lastIndexesRun - start) + "ms nvi:"
				// + NodeValueIndexes.size() + ";aoi:"
				// + ArcOpIndexes.size());
			}
		}
	}

	public void compactDB() {
		mergeNodeValueShards();
		mergeArcOpShards();
		loadIndexes(true);
	}

	public synchronized void mergeNodeValueShards() {
		System.out.println("mergeNodeValueShards called!");
		Query q = new Query("NodeValueShard").addSort("spread",
				Query.SortDirection.DESCENDING);
		ArrayList<NodeValueShard> list = new ArrayList<NodeValueShard>(500);
		int size = 0;
		PreparedQuery shardsQuery = datastore.prepare(q);
		Iterator<Entity> iter = shardsQuery.asIterable(withLimit(500))
				.iterator();
		while (iter.hasNext()) {
			Entity ent = iter.next();
			System.out.println("Checking entity: " + ent.getProperty("size")
					+ ":" + size);
			if (size + (Long) ent.getProperty("size") <= NodeValueBuffer.STORESIZE*10) { //not too many to prevent OOM
				size += (Long) ent.getProperty("size");
				list.add((NodeValueShard) MemoStorable.load(ent));
			} else {
				break;
			}
		}
		if (list.size() > 1) {
//			loadIndexes(false);
			System.out.println("Merge " + list.size() + " NodeValueShards!");
			NodeValueShard.devideAndMerge(list.toArray(new NodeValueShard[0]));
			memCache.put("memoNodes_lastUpdate", System.currentTimeMillis());
		}
	}

	public synchronized void mergeArcOpShards() {
		System.out.println("mergeArcOpShards called!");
		Query q = new Query("ArcOpShard").addSort("spread",
				Query.SortDirection.DESCENDING);
		ArrayList<ArcOpShard> list = new ArrayList<ArcOpShard>(100);
		int size = 0;
		PreparedQuery shardsQuery = datastore.prepare(q);
		Iterator<Entity> iter = shardsQuery.asIterable(withLimit(100))
				.iterator();
		while (iter.hasNext()) {
			Entity ent = iter.next();
//			System.out.println("Checking entity: " + ent.getProperty("size")
//					+ ":" + size);
			if (size + (Long) ent.getProperty("size") <= ArcOpBuffer.STORESIZE*10) {//Not too many to prevent OOM
				size += (Long) ent.getProperty("size");
				list.add((ArcOpShard) MemoStorable.load(ent));
			} else {
				break;
			}
		}
		if (list.size() > 1) {
//			loadIndexes(false);
			System.out.println("Merge " + list.size() + " ArcOpShards!");
			ArcOpShard.devideAndMerge(list.toArray(new ArcOpShard[0]));
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
			Collections.sort(ArcOpIndexes, Collections.reverseOrder());
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

	public ArrayList<NodeValueShard> getSparseNodeValueShards(long room) {
		ArrayList<NodeValueShard> result = new ArrayList<NodeValueShard>(Math.min((int) NodeValueShards.size(), 10));
		Iterator<NodeValueShard> iter;
		iter = ImmutableMap.copyOf(NodeValueShards.asMap()).values().iterator();
		int maxSize = NodeValueBuffer.STORESIZE*10;
		while (iter.hasNext()) {
			NodeValueShard shard = iter.next();
			if (shard.isDeleted())
				continue;
			if (shard.spread > 100000) {
				maxSize-=shard.getStoredSize();
				result.add(shard);
			}
			if (maxSize<0 || result.size()>10) break;
		}
		return result;
	}

	public ArrayList<ArcOpShard> getSparseArcOpShards(long room) {
		ArrayList<ArcOpShard> result = new ArrayList<ArcOpShard>((int) ArcOpShards.size());
		Iterator<ArcOpShard> iter;
		iter = ImmutableMap.copyOf(ArcOpShards.asMap()).values().iterator();
		int maxSize = ArcOpBuffer.STORESIZE*10;
		while (iter.hasNext()) {
			ArcOpShard shard = iter.next();
			if (shard.isDeleted())
				continue;
			if (shard.spread > 100000) {
				maxSize-=shard.getStoredSize();
				result.add(shard);
			}
			if (maxSize<0 || result.size()>10) break;
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
//					System.out.println("Removed index from NodeValueIndexes:"
//							+ NodeValueIndexes.size());
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
//					System.out.println("Removed index from ArcOpIndexes:"
//							+ ArcOpIndexes.size());
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
				NodeValueShard shard = null;
				try {
					shard = NodeValueShards.getUnchecked(index.getShardKey());
				} catch (Exception e) {
					System.err.println("missed a nv shard, merged db? "
							+ index.getShardKey());
					e.printStackTrace();
				}
				if (shard == null) {
					// Happens if shard has been deleted, indication that
					// indexes are outdated!
					if (retryCount > 10) {
						System.err
								.println("Warning, even after 10 retries no shard found!");
						index.delete(true);
					}
					System.out.println("retry loadIndexes for value shard."+uuid);
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
//		System.out.println("Looking for value of:"+uuid.toString());
		MemoWriteBus writeBus = MemoWriteBus.getBus();
		result = writeBus.values.findBefore(uuid, timestamp);
//		if (result != null) System.out.println("Found in writeBus:"+new String(result.getValue()));
		
		ImmutableList<NodeValueIndex> copy;
		synchronized (NodeValueIndexes) {
			if (NodeValueIndexes.size() == 0) {
//				System.out.println("Indexes empty, returning writeBus val");
				return result;
			}
			copy = ImmutableList.copyOf(NodeValueIndexes);
		}
		Iterator<NodeValueIndex> iter = copy.iterator();
		while (iter.hasNext()) {
			NodeValueIndex index = iter.next();
			if (result != null
					&& index.getNewest() < result.getTimestamp_long()) {
//				System.out.println("next index older than earlier found value");
				return result;
			}
//			System.out.println("Checking index:"+index.getOldest()+ " "+ timestamp + " "+ uuid.toString());
			if (index.getOldest() < timestamp && index.contains(uuid)) {
//				System.out.println("Found an index entry!");
				NodeValueShard shard = null;
				try {
					shard = NodeValueShards.getUnchecked(index.getShardKey());
				} catch (Exception e) {
					System.err.println("missed a nv shard, merged db? "
							+ index.getShardKey());
//					e.printStackTrace();
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
					System.out.println("retry loadIndexes for value shard."+uuid);
					loadIndexes(true);
					return getValue(uuid, timestamp, retryCount + 1);
				}
				if (shard != null) {
//					System.out.println("Here?");
					NodeValue res = shard.findBefore(uuid, timestamp);
					if (res == null) {
//						System.out.println("Value wasn't in this shard, trying next."+uuid);
						continue;
					}
					if (result == null
							|| res.getTimestamp_long() > result
									.getTimestamp_long()) {
//						System.out.println("New value found in shard:"+new String(res.getValue()));
						result = res;
					}
				}
			}
		}
		//System.out.println("Normal return:"+uuid+":"+new String(result.getValue()));
//		if (result == null) System.out.println("Warning, couldn't find value anywhere! Returning null");
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
				if (op.getTimestamp_long() <= timestamp) {
					wbresult.add(op);
				}
			}
			break;
		case 1:
			List<ArcOp> parents = MemoWriteBus.getBus().ops.getParentOps(uuid);
			wbresult.ensureCapacity(wbresult.size() + parents.size() + 1);
			for (ArcOp op : parents) {
				if (op.getTimestamp_long() <= timestamp) {
					wbresult.add(op);
				}
			}
			break;
		}
		synchronized (ArcOpIndexes) {
			if (ArcOpIndexes.size() > 0) {
				doIndexes = true;
				copy = ImmutableList.copyOf(ArcOpIndexes);
			}
		}
		if (doIndexes) {
			Iterator<ArcOpIndex> iter = copy.iterator();
			while (iter.hasNext()) {
				ArcOpIndex index = iter.next();
				if (uuid.time != 0 && index.getStoreTime() < MemoUtils.gettime(uuid)) {
					break;
				}
				switch (type) {
				case 0: // parentList, UUID is child
					if (index.containsChild(uuid)) {
//						System.out.println("index:"+index.getMyKey()+ "contains:"+uuid);
						ArcOpShard shard = null;
						try {
							shard = ArcOpShards.getUnchecked(index
									.getShardKey());
						} catch (Exception e) {
							System.err
									.println("missed a ops shard, merged db? "
											+ index.getShardKey());
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
						//						System.out.println("index:"+index.getMyKey()+ "contains:"+uuid);
						ArcOpShard shard = null;
						try {
							shard = ArcOpShards.getUnchecked(index
									.getShardKey());
						} catch (Exception e) {
							System.err
									.println("missed a ops shard, merged db? "
											+ index.getShardKey());
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
