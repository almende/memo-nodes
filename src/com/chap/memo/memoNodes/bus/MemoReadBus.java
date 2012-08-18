package com.chap.memo.memoNodes.bus;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;

public class MemoReadBus {

	static Cache<Key, NodeValueShard> NodeValueShards = CacheBuilder
			.newBuilder().maximumSize(10).build();
	static Cache<Key, ArcOpShard> ArcOpShards = CacheBuilder.newBuilder()
			.maximumSize(100).build();

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
		loadIndexes(false, 0);
	};

	void updateIndexes() {
		loadIndexes(false, lastIndexesRun - 10000);
	}

	public void delShard(NodeValueShard oldShard) {
		NodeValueShards.invalidate(oldShard.getMyKey());
	}

	public void delShard(ArcOpShard oldShard) {
		ArcOpShards.invalidate(oldShard.getMyKey());
	}

	void loadIndexes(boolean clear, long sinceTimestamp) {
		System.out.println("reloading indexes:"+clear+":"+sinceTimestamp);
		long start = System.currentTimeMillis();
		
		synchronized (NodeValueIndexes) {
			synchronized (ArcOpIndexes) {
				System.out.println("Started out with: nvi:"
						+ NodeValueIndexes.size() + ";aoi:"
						+ ArcOpIndexes.size());
				if (clear) {
					NodeValueIndexes.clear();
					NodeValueShards.invalidateAll();
					ArcOpIndexes.clear();
					ArcOpShards.invalidateAll();
					lastValueChange = System.currentTimeMillis();
					lastOpsChange = System.currentTimeMillis();
					memCache.delete("memoNodes_lastUpdate");
				} else {
					Object lastUpdate = memCache.get("memoNodes_lastUpdate");
					if (lastUpdate != null) {
						long lastUpdateTime = (Long) lastUpdate;
						if (lastUpdateTime <= lastIndexesRun) {
							System.out.println("No update of indexes necessary");
							return; // Nothing new to expect;
						}
					}
				}

				Query q = new Query("NodeValueIndex").addSort("timestamp",
						Query.SortDirection.DESCENDING);
				if (sinceTimestamp > 0) {
					// q.setFilter(new Query.FilterPredicate("timestamp",
					// FilterOperator.GREATER_THAN_OR_EQUAL, sinceTimestamp));
					q.addFilter("timestamp",
							FilterOperator.GREATER_THAN_OR_EQUAL,
							sinceTimestamp);
				}
				PreparedQuery NodeValueIndexQuery = datastore.prepare(q);
				QueryResultList<Entity> rl = NodeValueIndexQuery
						.asQueryResultList(withLimit(1000));
				if (rl.size() > 0) {
					Iterator<NodeValueIndex> iter = NodeValueIndexes.iterator();
					while (iter.hasNext()){
						NodeValueIndex index = iter.next();
						if (index.getNewest() >= sinceTimestamp){
							iter.remove();
						} else {
							break;
						}
					}
					for (Entity ent : rl) {
						MemoStorable obj = MemoStorable.load(ent);
						if (obj != null) {
							NodeValueIndex index = (NodeValueIndex) obj;
							NodeValueIndexes.add(0,index);
						}
					}
					lastValueChange = System.currentTimeMillis();
				}
				Collections.sort(NodeValueIndexes, Collections.reverseOrder());

				q = new Query("ArcOpIndex").addSort("timestamp");
				if (sinceTimestamp > 0) {
					// q.setFilter(new Query.FilterPredicate("timestamp",
					// FilterOperator.GREATER_THAN_OR_EQUAL, sinceTimestamp));
					q.addFilter("timestamp",
							FilterOperator.GREATER_THAN_OR_EQUAL,
							sinceTimestamp);
				}
				PreparedQuery ArcOpIndexQuery = datastore.prepare(q);
				rl = ArcOpIndexQuery.asQueryResultList(withLimit(1000));
				if (rl.size() > 0) {
					Iterator<ArcOpIndex> iter = ArcOpIndexes.iterator();
					while (iter.hasNext()){
						ArcOpIndex index = iter.next();
						if (index.getStoreTime() >= sinceTimestamp){
							iter.remove();
						}
					}
					for (Entity ent : rl) {
						ArcOpIndex index = (ArcOpIndex) MemoStorable.load(ent);
						ArcOpIndexes.add(index);
					}
					lastOpsChange = System.currentTimeMillis();
				}
				lastIndexesRun = System.currentTimeMillis();

				System.out.println("Done reloading indexes:"
						+ (lastIndexesRun - start) + "ms nvi:"
						+ NodeValueIndexes.size() + ";aoi:"
						+ ArcOpIndexes.size());
			}
		}

	}

	public void addNodeValueIndex(NodeValueIndex index, NodeValueShard shard) {
		NodeValueShards.put(index.getShardKey(), shard);
		synchronized (NodeValueIndexes) {
			NodeValueIndexes.add(0,index);
			Collections.sort(NodeValueIndexes, Collections.reverseOrder());
		}
		lastValueChange = System.currentTimeMillis();
		memCache.put("memoNodes_lastUpdate",lastValueChange);
	}

	public void addOpsIndex(ArcOpIndex index, ArcOpShard shard) {
		ArcOpShards.put(index.getShardKey(), shard);
		synchronized (ArcOpIndexes) {
			ArcOpIndexes.add(index);
		}
		lastOpsChange = System.currentTimeMillis();
		memCache.put("memoNodes_lastUpdate",lastOpsChange);
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
		Iterator<NodeValueShard> iter;
		iter = ImmutableMap.copyOf(NodeValueShards.asMap()).values()
				.iterator();
		while (iter.hasNext()) {
			NodeValueShard shard = iter.next();
			if (shard.isDeleted())
				continue;
			
			if (shard.nodes.size() < NodeValueBuffer.SHARDSIZE - room) {
				result = shard;
				break;
			}
		}
		return result;
	}

	public ArcOpShard getSparseArcOpShard(int room) {
		ArcOpShard result = null;
		Iterator<ArcOpShard> iter;
		iter = ImmutableMap.copyOf(ArcOpShards.asMap()).values().iterator();
		while (iter.hasNext()) {
			ArcOpShard shard = iter.next();
			if (shard.isDeleted())
				continue;
			if (shard.parents.size() < ArcOpBuffer.SHARDSIZE - room) {
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
			if (index.getNodeIds().contains(uuid)) {
				NodeValueShard shard = NodeValueShards.getIfPresent(index
						.getShardKey());
				if (shard == null) {
					shard = (NodeValueShard) MemoStorable.load(index
							.getShardKey());
					if (shard != null) {
						NodeValueShards.put(shard.getMyKey(), shard);
					}
				}
				if (shard == null) {
					// Happens if shard has been deleted, indication that
					// indexes are outdated!
					if (retryCount > 10) {
						System.err
								.println("Warning, even after 10 retries no shard found!");
						return null;
					}

					loadIndexes(true, 0);
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
		
//		if (result != null) System.out.println("Result from writeBus:"+uuid+":"+new String(result.getValue()));
		ImmutableList<NodeValueIndex> copy;
		synchronized (NodeValueIndexes) {
			if (NodeValueIndexes.size() == 0){
//				System.out.println("early return as indexes are empty!."+uuid);
				return result;
			}
			// Iterator through indexes, copy because stuff might disappear
			copy = ImmutableList.copyOf(NodeValueIndexes);
		}
		Iterator<NodeValueIndex> iter = copy.iterator();
		while (iter.hasNext()) {
			NodeValueIndex index = iter.next();
			if (result != null
					&& index.getNewest() < result.getTimestamp_long()){
//				System.out.println("Early out as index.getNewest too old!."+uuid+":"+new String(result.getValue()));
				return result;
			}
			if (index.getOldest() < timestamp
					&& index.getNodeIds().contains(uuid)) {
				NodeValueShard shard = NodeValueShards.getIfPresent(index
						.getShardKey());
				if (shard == null) {
//					System.out.println("Needed to load shard."+uuid);
					shard = (NodeValueShard) MemoStorable.load(index
							.getShardKey());
					if (shard != null) {
						NodeValueShards.put(shard.getMyKey(), shard);
					}
				}
				if (shard == null) {
					// Happens if shard has been deleted, indication
					// that indexes are outdated!
					if (retryCount > 10) {
						System.err
								.println("Warning, even after 10 retries still no shard found!."+uuid);
						return null;
					}
//					System.out.println("retry loadIndexes for value shard."+uuid);
					loadIndexes(true, 0);
					return getValue(uuid, timestamp, retryCount + 1);
				}
				if (shard != null) {
					NodeValue res = shard.findBefore(uuid, timestamp);
					if (res == null){
//						System.out.println("Value wasn't in this shard, trying next."+uuid);
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
//		System.out.println("Normal return:"+uuid+":"+new String(result.getValue()));
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
					if (index.getChildren().contains(uuid)) {
						ArcOpShard shard = ArcOpShards.getIfPresent(index
								.getShardKey());
						if (shard == null) {
							System.out.println("Need to load ops shard!"+uuid);
							shard = (ArcOpShard) MemoStorable.load(index
									.getShardKey());
							if (shard != null) {
								ArcOpShards.put(shard.getMyKey(), shard);
							}
						}
						if (shard == null) {
							// Happens if shard has been deleted, indication
							// that indexes are outdated!
							if (retryCount > 10) {
								System.err
										.println("Warning, even after 10 retries still no shard found!"+uuid);
								return null;
							}
							loadIndexes(true, 0);
							System.err.println("Doing retry" + retryCount+" uuid"+uuid);
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
					if (index.getParents().contains(uuid)) {
						ArcOpShard shard = ArcOpShards.getIfPresent(index
								.getShardKey());
						if (shard == null) {
							System.out.println("Need to load shard!"+uuid);
							shard = (ArcOpShard) MemoStorable.load(index
									.getShardKey());
							if (shard != null) {
								ArcOpShards.put(shard.getMyKey(), shard);
							}
						}
						if (shard == null) {
							// Happens if shard has been deleted, indication
							// that indexes are outdated!
							if (retryCount > 10) {
								System.err
										.println("Warning, even after 10 retries still no shard found!");
								return null;
							}
							loadIndexes(true, 0);
							System.err.println("Doing retry" + retryCount+" uuid:"+uuid);
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
		Collections.sort(result);
		return result;
	}

}
