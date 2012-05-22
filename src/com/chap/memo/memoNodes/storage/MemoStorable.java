package com.chap.memo.memoNodes.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import static com.google.appengine.api.datastore.FetchOptions.Builder.*;

public abstract class MemoStorable implements Serializable,
		Comparable<MemoStorable> {
	private static final long serialVersionUID = -5770613002073776843L;
	static final DatastoreService datastore = DatastoreServiceFactory
			.getDatastoreService();
	Key myKey = null;
	long storeTime;
	long nanoTime;

	private byte[] serialize() {
		byte[] result = null;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ZipOutputStream zos = new ZipOutputStream(bos);
		try {
			// zos.setLevel(4);
			zos.putNextEntry(new ZipEntry("Object"));
			ObjectOutputStream oos = new ObjectOutputStream(zos);
			// ObjectOutputStream oos = new ObjectOutputStream(bos);
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

	public void delete() {
		if (myKey != null)
			delete(myKey);
	}

	public void delete(Key key) {
		try {
			Entity ent = datastore.get(key);
			if (ent.hasProperty("next")) {
				delete((Key) ent.getProperty("next")); // recurse
			}
			datastore.delete(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Key store(String type) {
		return store(null, type, new Date().getTime());
	}

	public Key store(String type, long storeDate) {
		return store(null, type, storeDate);
	}

	public Key store(Key orig, String type) {
		return store(orig, type, new Date().getTime());
	}

	public Key store(Key orig, String type, long storeDate) {
		final int MAXSIZE = 1000000;
		Entity ent;
		Key next = null;
		this.storeTime = storeDate;
		this.nanoTime = System.nanoTime();

		byte[] data = this.serialize();
		int length = data.length;
		int pointer = 0;
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
			next = ent.getKey();
			pointer += MAXSIZE;
		}

		if (orig != null) {
			ent = new Entity(orig);
		} else {
			ent = new Entity(type);
		}
		ent.setUnindexedProperty("payload",
				new Blob(Arrays.copyOfRange(data, pointer, length)));
		if (next != null)
			ent.setUnindexedProperty("next", next);
		ent.setProperty("timestamp", this.storeTime);
		datastore.put(ent);
		myKey = ent.getKey();
		// Try to force index writing
		try {
			datastore.get(myKey);
		} catch (EntityNotFoundException e) {
		}
		return myKey;
	}

	// Factory methods:
	public static MemoStorable load(Entity ent) {
		byte[] result;
		Key key = ent.getKey();
		try {
			Blob blob = (Blob) ent.getProperty("payload");
			byte[] data = blob.getBytes();
			result = Arrays.copyOf(data, data.length);

			while (ent.hasProperty("next")) {
				// Expensive, should not be used too much!
				Key next = (Key) ent.getProperty("next");
				ent = datastore.get(next);
				blob = (Blob) ent.getProperty("payload");
				data = blob.getBytes();
				result = concat(data, result); // Add to front of result, due to
												// the storing order
			}

		} catch (EntityNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		MemoStorable res = _unserialize(result);
		res.myKey = key;
		return res;

	}

	public static MemoStorable load(Key key) {
		try {
			Entity ent = datastore.get(key);
			return load(ent);
		} catch (EntityNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static ArrayList<MemoStorable> getChanges(String type, Date after) {

		Query q = new Query(type).addFilter("timestamp",
				FilterOperator.GREATER_THAN_OR_EQUAL, after.getTime());
		PreparedQuery pq = datastore.prepare(q);
		ArrayList<MemoStorable> result = new ArrayList<MemoStorable>(
				pq.countEntities(withLimit(1000)));
		Iterator<Entity> iter = pq.asIterator();
		while (iter.hasNext()) {
			Entity ent = iter.next();
			result.add(load(ent));
		}
		return result;
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

	private static MemoStorable _unserialize(byte[] data) {
		MemoStorable result = null;
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		ZipInputStream zis = new ZipInputStream(bis);
		try {
			zis.getNextEntry();
			ObjectInputStream ios = new ObjectInputStream(zis);
			// ObjectInputStream ios = new ObjectInputStream(bis);
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
}
