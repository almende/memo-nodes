package com.chap.memo.memoNodes.storage;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public abstract class MemoStorable implements Serializable,
		Comparable<MemoStorable> {
	private static final long serialVersionUID = -5770613002073778843L;
	protected final static Logger log = Logger.getLogger(MemoStorable.class.getName());
	
	static Cache<Key,MemoStorable> deletedCache = CacheBuilder.newBuilder()
			.maximumSize(10)
			.expireAfterWrite(10,TimeUnit.SECONDS)
			.build();
	static final DatastoreService datastore = DatastoreServiceFactory
			.getDatastoreService();
	
	
	Key myKey = null;
	long storeTime;
	long nanoTime;
	transient int storedSize;
	public long spread;
	
	public int getStoredSize(){
		return storedSize;
	}
	
	private byte[] serialize() {
		byte[] result = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream(150000);
		try {
//			GZIPOutputStream zos = new GZIPOutputStream(bos);
			ZipOutputStream zos = new ZipOutputStream(bos);
			// zos.setLevel(4);
			zos.putNextEntry(new ZipEntry("Object"));
			ObjectOutputStream oos = new ObjectOutputStream(zos);
//			oos.writeInt(1);
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
			log.severe("Failed to serialize MemoStorable: "+e.getMessage());
			log.severe(e.getCause().getMessage());
		}
		return result;
	}
	private static MemoStorable _unserialize(byte[] data, int size) {
		MemoStorable result = null;
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		
		try {
			ZipInputStream zis = new ZipInputStream(bis);
			zis.getNextEntry();
			BufferedInputStream bus = new BufferedInputStream(zis,size>0?size:15000);
			ObjectInputStream ios = new ObjectInputStream(bus);
//			ios.readInt();
//			System.out.println("Non-blocking available: "+bis.available()+":"+zis.available()+":"+bus.available());
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
		
	public boolean isDeleted(){
		if (myKey == null) return true;
		return (deletedCache.getIfPresent(myKey) != null);
	}

	public void delete() {
		if (myKey != null)
			delete(myKey);
	}

	public void delete(boolean fullDrop){
		if (myKey != null){
			delete(myKey);
			if (fullDrop){
				deletedCache.invalidate(myKey);
			}
		}
	}
	
	public void delete(Key key) {
		if (deletedCache.getIfPresent(key) != null) return;
		if (myKey.equals(key)){
			deletedCache.put(key, this);
		}
		try {
			Entity ent = datastore.get(key);
			if (ent.hasProperty("next")) {
				delete((Key) ent.getProperty("next")); // recurse
			}
			datastore.delete(key);
			try {
				datastore.get(myKey);
			} catch (EntityNotFoundException e) {
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

	public Key store(String type) {
		return store(null, type, System.currentTimeMillis());
	}

	public Key store(String type, long storeDate) {
		return store(null, type, storeDate);
	}

	public Key store(Key orig, String type) {
		return store(orig, type, System.currentTimeMillis());
	}
	
	public Key store(Key orig, String type, long storeDate) {
//		long start = System.currentTimeMillis();
		final int MAXSIZE = 1000000;
		Entity ent;
		Key next = null;
		this.storeTime = storeDate;
		this.nanoTime = System.nanoTime();

		byte[] data = this.serialize();
		Integer length = data.length;
		int pointer = 0;
//		int counter = 0;
		while (length - pointer >= MAXSIZE) {
			// Expensive, should not be used too much!
			ent = new Entity(type + "_fragment");
			ent.setUnindexedProperty(
					"payload",
					new Blob(Arrays.copyOfRange(data, pointer, pointer
							+ MAXSIZE)));
			if (next != null)
				ent.setUnindexedProperty("next", next);
			datastore.put(ent);
//			counter++;
			next = ent.getKey();
			pointer += MAXSIZE;
		}

		if (orig != null) {
			System.err.println("Warning, storing storable twice! Strange, should not happen with our immutable structures.");
			ent = new Entity(orig);
		} else {
			ent = new Entity(type);
		}
		ent.setUnindexedProperty("payload",
				new Blob(Arrays.copyOfRange(data, pointer, length)));
		if (next != null)
			ent.setUnindexedProperty("next", next);
		ent.setProperty("timestamp", this.storeTime);
		ent.setProperty("size", length);
		ent.setProperty("spread",this.spread);
		datastore.put(ent);
//		counter++;
		myKey = ent.getKey();
		// Try to force index writing
		try {
			datastore.get(myKey);
		} catch (EntityNotFoundException e) {
		}
		this.storedSize=length;
		//System.out.println("Just stored shard of "+length+ " bytes in "+counter+" fragments  in "+(System.currentTimeMillis()-start)+" ms");
		return myKey;
	}

	// Factory methods:
	public static MemoStorable load(Entity ent) {
//		long start = System.currentTimeMillis();
		byte[] result;
		Key key = ent.getKey();
		int size=0;
		long spread = 0;
//		int count=0;
		try {
			Blob blob = (Blob) ent.getProperty("payload");
			size = (int)((Long) ent.getProperty("size")%Integer.MAX_VALUE);
			spread = ((Long) ent.getProperty("spread"));
			
			byte[] data = blob.getBytes();
			result = Arrays.copyOf(data, data.length);
//			count++;
			while (ent.hasProperty("next")) {
				// Expensive, should not be used too much!
				Key next = (Key) ent.getProperty("next");
				ent = datastore.get(next);
				blob = (Blob) ent.getProperty("payload");
				data = blob.getBytes();
				result = concat(data, result); // Add to front of result, due to
//				count++;						// the storing order
			}

		} catch (EntityNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		MemoStorable res = _unserialize(result,size);
		res.spread = spread;
		res.storedSize=result.length;
		if (res != null) res.myKey = key;
		return res;

	}

	public static MemoStorable load(Key key) {
		MemoStorable res =deletedCache.getIfPresent(key);
		if (res != null) return res;
		try {
			Entity ent = datastore.get(key);
			return load(ent);
		} catch (EntityNotFoundException e) {
			return null;
		}
	}

	public long getStoreTime() {
		return storeTime;
	}

	public long getNanoTime() {
		return nanoTime;
	}

	@Override
	public int compareTo(MemoStorable other) {
		if (myKey != null && other.myKey != null && myKey.equals(other.myKey)) {
			return 0;
		}
		if (this.storeTime == other.storeTime) {
			return (int) ((this.nanoTime - other.nanoTime) % Integer.MAX_VALUE);
		}
		return (int) ((this.storeTime - other.storeTime) % Integer.MAX_VALUE);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof MemoStorable) {
			MemoStorable other = (MemoStorable) o;
			if (myKey != null && other.myKey != null
					&& myKey.equals(other.myKey)) {
				return true;
			}
			if ((myKey == null || other.myKey == null)
					&& this.storeTime == other.storeTime) {
				return (this.nanoTime == other.nanoTime);
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return (int) (myKey.hashCode() * this.nanoTime) % Integer.MAX_VALUE;
	}

	// Tools:
	private static byte[] concat(byte[]... arrays) {
		// count total number of items in the resulting array.
		int length = 0;
		for (int i = 0; i < arrays.length; i++) {
			length += arrays[i].length;
		}
		byte[] result = new byte[length];
		int k = 0;
		for (int i = 0; i < arrays.length; i++) {
			for (int j = 0; j < arrays[i].length; j++) {
				result[k++] = arrays[i][j];
			}
		}
		return result;
	}

	public Key getMyKey() {
		return myKey;
	}

	public void setMyKey(Key myKey) {
		this.myKey = myKey;
	}
	
	public abstract int getSize();
}
