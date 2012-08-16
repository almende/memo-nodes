package com.chap.memo.memoNodes.bus;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.chap.memo.memoNodes.MemoNode;
import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.NodeValue;
import com.chap.memo.memoNodes.storage.ArcOpIndex;
import com.chap.memo.memoNodes.storage.ArcOpShard;
import com.chap.memo.memoNodes.storage.MemoStorable;
import com.chap.memo.memoNodes.storage.NodeValueIndex;
import com.chap.memo.memoNodes.storage.NodeValueShard;
import com.chap.memo.memoNodes.util.MyMap;
import com.eaio.uuid.UUID;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

public class MemoReadBus {
	static MyMap<Key, NodeValueShard> innerMap_NodeValueShards = new MyMap<Key, NodeValueShard>(
			10, new Float(0.75), true);
	static Map<Key, NodeValueShard> NodeValueShards = Collections
			.synchronizedMap(innerMap_NodeValueShards);

	static MyMap<Key, ArcOpShard> innerMap_ArcOpShards = new MyMap<Key, ArcOpShard>(
			10, new Float(0.75), true);
	static Map<Key, ArcOpShard> ArcOpShards = Collections
			.synchronizedMap(innerMap_ArcOpShards);

	public List<NodeValueIndex> NodeValueIndexes = Collections
			.synchronizedList(new ArrayList<NodeValueIndex>(100));
	public List<ArcOpIndex> ArcOpIndexes = Collections
			.synchronizedList(new ArrayList<ArcOpIndex>(100));

	DatastoreService datastore = null;
	MemcacheService memCache = null;
	long lastValueChange = System.currentTimeMillis();
	long lastOpsChange = System.currentTimeMillis();
	long lastIndexesRun = 0;

	private final static MemoReadBus bus = new MemoReadBus();

	MemoReadBus() {
		innerMap_NodeValueShards.synchronization_anchor = NodeValueShards;
		innerMap_ArcOpShards.synchronization_anchor = ArcOpShards;
		loadIndexes(false, 0);
	};

	void updateIndexes() {
		loadIndexes(false, lastIndexesRun - 10000);
	}

	void delShard(NodeValueShard oldShard) {
		synchronized (NodeValueShards) {
			NodeValueShards.remove(oldShard.getMyKey());
		}
	}

	void delShard(ArcOpShard oldShard) {
		synchronized (ArcOpShards) {
			ArcOpShards.remove(oldShard.getMyKey());
		}
	}

	void loadIndexes(boolean clear, long sinceTimestamp) {
		if (memCache == null) {
			memCache = MemcacheServiceFactory.getMemcacheService();
		}
		if (clear) {
			synchronized (NodeValueIndexes) {
				NodeValueIndexes.clear();
			}
			synchronized (NodeValueShards) {
				NodeValueShards.clear();
			}
			synchronized (ArcOpIndexes) {
				ArcOpIndexes.clear();
			}
			synchronized (ArcOpShards) {
				ArcOpShards.clear();
			}
			lastValueChange = System.currentTimeMillis();
			lastOpsChange = System.currentTimeMillis();
			memCache.delete("memoNodes_lastUpdate");
		} else {
			Object lastUpdate = memCache.get("memoNodes_lastUpdate");
			if (lastUpdate != null) {
				long lastUpdateTime = (Long) lastUpdate;
				if (lastUpdateTime <= lastIndexesRun) {
					return; // Nothing new to expect;
				}
			}
		}
		if (datastore == null) {
			datastore = DatastoreServiceFactory.getDatastoreService();
		}
		Query q = new Query("NodeValueIndex").addSort("timestamp",
				Query.SortDirection.DESCENDING);
		if (sinceTimestamp > 0) {
			q.setFilter(new Query.FilterPredicate("timestamp",
					FilterOperator.GREATER_THAN_OR_EQUAL, sinceTimestamp));
		}
		PreparedQuery NodeValueIndexQuery = datastore.prepare(q);
		QueryResultList<Entity> rl = NodeValueIndexQuery
				.asQueryResultList(withLimit(100));
		if (rl.size() > 0) {
			for (Entity ent : rl) {
				MemoStorable obj = MemoStorable.load(ent);
				if (obj != null) {
					NodeValueIndex index = (NodeValueIndex) obj;
					synchronized(NodeValueIndexes){
						NodeValueIndexes.add(index);
					}
				}
			}
			lastValueChange = System.currentTimeMillis();
		}

		q = new Query("ArcOpIndex").addSort("timestamp");
		if (sinceTimestamp > 0) {
			q.setFilter(new Query.FilterPredicate("timestamp",
					FilterOperator.GREATER_THAN_OR_EQUAL, sinceTimestamp));
		}
		PreparedQuery ArcOpIndexQuery = datastore.prepare(q);
		rl = ArcOpIndexQuery.asQueryResultList(withLimit(1000));
		if (rl.size() > 0) {
			for (Entity ent : rl) {
				ArcOpIndex index = (ArcOpIndex) MemoStorable.load(ent);
				synchronized(ArcOpIndexes){
					ArcOpIndexes.add(index);
				}
			}
			lastOpsChange = System.currentTimeMillis();
		}
		lastIndexesRun = System.currentTimeMillis();
	}

	void addValueIndex(NodeValueIndex index, NodeValueShard shard) {
		// System.out.println("adding index to NodeValueIndexes");
		synchronized (NodeValueShards) {
			NodeValueShards.put(index.getShardKey(), shard);
		}
		synchronized (NodeValueIndexes) {
			NodeValueIndexes.add(index);
		}
		lastValueChange = System.currentTimeMillis();
	}

	void addOpsIndex(ArcOpIndex index, ArcOpShard shard) {
		synchronized (ArcOpShards) {
			ArcOpShards.put(index.getShardKey(), shard);
		}
		synchronized (ArcOpIndexes) {
			ArcOpIndexes.add(index);
		}
		lastOpsChange = System.currentTimeMillis();
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

	public NodeValueShard getSparseNodeValueShard(int room) {
		NodeValueShard result = null;
		synchronized (NodeValueShards) {
			for (NodeValueShard shard : NodeValueShards.values()) {
				if (shard.isDeleted()) continue;
				if (shard.getCurrentSize() < NodeValueShard.SHARDSIZE - room) {
					result = shard;
					break;
				}
			}
		}
		return result;
	}

	public ArcOpShard getSparseArcOpShard(int room) {
		ArcOpShard result = null;
		synchronized (ArcOpShards) {
			for (ArcOpShard shard : ArcOpShards.values()) {
				if (shard.isDeleted()) continue;
				if (shard.getCurrentSize() < ArcOpShard.SHARDSIZE - room) {
					result = shard;
					break;
				}
			}
		}
		return result;
	}

	public NodeValueIndex removeNodeValueIndexByShard(Key shardKey) {
		synchronized (NodeValueIndexes) {
			Iterator<NodeValueIndex> iter = NodeValueIndexes.iterator();
			while (iter.hasNext()) {
				NodeValueIndex index = iter.next();
				if (index.getShardKey().equals(shardKey)){
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
				if (index.getShardKey().equals(shardKey)){
					iter.remove();
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

		if (NodeValueIndexes.size() <= 0)
			return result;
		synchronized (NodeValueIndexes) {
			Iterator<NodeValueIndex> iter = NodeValueIndexes.iterator();
			while (iter.hasNext()) {
				NodeValueIndex index = iter.next();
				if (index.getNodeIds().contains(uuid)) {
					NodeValueShard shard = null;
					synchronized (NodeValueShards) {
						if (NodeValueShards.containsKey(index.getShardKey())) {
							shard = NodeValueShards.get(index.getShardKey());
						}
					}
					if (shard == null) {
						shard = (NodeValueShard) MemoStorable.load(index
								.getShardKey());
						if (shard != null) {
							synchronized (NodeValueShards) {
								NodeValueShards.put(shard.getMyKey(), shard);
							}
						}
					}
					if (shard == null) {
						// Happens if shard has been deleted, indication that
						// indexes are outdated!
						if (retryCount>10){
							System.out.println("Warning, even after 10 retries no shard found!");
							return null;
						}

						loadIndexes(true, 0);
						return findAll(uuid, retryCount+1);
					}
					if (shard != null) {
						for (NodeValue nv : shard.findAll(uuid)) {
							result.add(new MemoNode(nv));
						}
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
		return getValue(uuid, timestamp, false);
	}

	public NodeValue getValue(UUID uuid, long timestamp, boolean onRetry) {
		NodeValue result = null;

		MemoWriteBus writeBus = MemoWriteBus.getBus();
		result = writeBus.values.findBefore(uuid, timestamp);

		if (NodeValueIndexes.size() == 0)
			return result;
		synchronized (NodeValueIndexes) {
			Iterator<NodeValueIndex> iter = NodeValueIndexes.iterator();
			while (iter.hasNext()) {
				NodeValueIndex index = iter.next();
				if (result == null
						|| index.getNewest() > result.getTimestamp_long()) {
					if (index.getOldest() < timestamp
							&& index.getNodeIds().contains(uuid)) {
						NodeValueShard shard = null;
						synchronized (NodeValueShards) {
							if (NodeValueShards
									.containsKey(index.getShardKey())) {
								shard = NodeValueShards
										.get(index.getShardKey());
							}
						}
						if (shard == null) {
							shard = (NodeValueShard) MemoStorable.load(index
									.getShardKey());
							if (shard != null) {
								synchronized (NodeValueShards) {
									NodeValueShards
											.put(shard.getMyKey(), shard);
								}
							}
						}
						if (shard == null) {
							// Happens if shard has been deleted, indication
							// that indexes are outdated!
							if (onRetry)
								return null;// only retry once!

							loadIndexes(true, 0);
							return getValue(uuid, timestamp, true);
						}
						if (shard != null) {
							NodeValue res = shard.findBefore(uuid, timestamp);
							if (result == null
									|| res.getTimestamp_long() > result
											.getTimestamp_long()) {
								result = res;
							}
						}
					}
				}
			}
		}
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
		ArrayList<ArcOp> result = new ArrayList<ArcOp>(10);
		if (ArcOpIndexes.size() > 0
				&& ArcOpIndexes.get(ArcOpIndexes.size() - 1).getStoreTime() >= since) {
			synchronized (ArcOpIndexes) {
				Iterator<ArcOpIndex> iter = ArcOpIndexes.iterator();
				while (iter.hasNext()) {
					ArcOpIndex index = iter.next();
					if (index.getStoreTime() < since) {
						continue;
					}
					switch (type) {
					case 0: // parentList, UUID is child
						if (index.getChildren().contains(uuid)) {
							ArcOpShard shard = null;
							synchronized (ArcOpShards) {
								if (ArcOpShards
										.containsKey(index.getShardKey())) {
									shard = ArcOpShards
											.get(index.getShardKey());
								}
							}
							if (shard == null) {
								// System.out.println("Need to load shard!");
								shard = (ArcOpShard) MemoStorable.load(index
										.getShardKey());
								if (shard != null) {
									synchronized (ArcOpShards) {
										ArcOpShards
												.put(shard.getMyKey(), shard);
									}
								}
							}
							if (shard == null) {
								// Happens if shard has been deleted, indication
								// that indexes are outdated!
								if (retryCount>10){
									System.out.println("Warning, even after 10 retries still no shard found!");
									return null;// only retry once!
								}
								loadIndexes(true, 0);
								return getOps(uuid, type, timestamp, since,
										retryCount+1);
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
						if (index.getParents().contains(uuid)) {
							ArcOpShard shard = null;
							synchronized (ArcOpShards) {
								if (ArcOpShards
										.containsKey(index.getShardKey())) {
									shard = ArcOpShards
											.get(index.getShardKey());
								}
							}
							if (shard == null) {
								// System.out.println("Need to load shard!");
								shard = (ArcOpShard) MemoStorable.load(index
										.getShardKey());
								if (shard != null) {
									synchronized (ArcOpShards) {
										ArcOpShards
												.put(shard.getMyKey(), shard);
									}
								}
							}
							if (shard == null) {
								// Happens if shard has been deleted, indication
								// that indexes are outdated!
								if (retryCount>10){
									System.out.println("Warning, even after 10 retries still no shard found!");
									return null;// only retry once!
								}
								loadIndexes(true, 0);
								return getOps(uuid, type, timestamp, since,
										retryCount+1);
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
		}
		// long half = System.currentTimeMillis();
		switch (type) {
		case 0: // parentList, UUID is child
			List<ArcOp> children = MemoWriteBus.getBus().ops.getChildOps(uuid);
			result.ensureCapacity(result.size() + children.size() + 1);
			for (ArcOp op : children) {
				if (op.getTimestamp_long() <= timestamp
						&& op.getTimestamp_long() >= since) {
					result.add(op);
				}
			}
			break;
		case 1:
			List<ArcOp> parents = MemoWriteBus.getBus().ops.getParentOps(uuid);
			result.ensureCapacity(result.size() + parents.size() + 1);
			for (ArcOp op : parents) {
				if (op.getTimestamp_long() <= timestamp
						&& op.getTimestamp_long() >= since) {
					result.add(op);
				}
			}
			break;
		}
		// long almost = System.currentTimeMillis();
		Collections.sort(result);
		/*
		 * long done = System.currentTimeMillis(); count++; if (done-start >=
		 * 2){
		 * System.out.println("GetOps: ("+count+")"+ArcOpIndexes.size()+"/"+result
		 * .size()+"->"+(half-start)+ ":"+(almost-start)+":"+(done-start)); }
		 */
		return result;
	}

}
