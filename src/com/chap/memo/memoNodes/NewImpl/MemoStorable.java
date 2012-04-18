package com.chap.memo.memoNodes.NewImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;

abstract class MemoStorable implements Serializable {
	private static final long serialVersionUID = -5770613002073776843L;
	static final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
	
	private byte[] serialize() {
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
	public Key store(String type){
		return store(null,type);
	}
	public Key store(Key orig,String type){
		final int MAXSIZE=1000000;
		Entity ent;
		Key next = null;
		
		byte[] data = this.serialize();
		int length = data.length;
		int pointer = 0;
		while (length-pointer >= MAXSIZE){
			//Expensive, should not be used too much!
			ent= new Entity(type+"_fragment");
			ent.setUnindexedProperty("payload", new Blob(Arrays.copyOfRange(data, pointer, pointer+MAXSIZE)));
			if (next != null) ent.setUnindexedProperty("next",next);
			datastore.put(ent);
			next = ent.getKey();
			pointer+=MAXSIZE;
		}
		
		if (orig != null) {
			ent = new Entity(orig);
		} else {
			ent = new Entity(type);
		}
		ent.setUnindexedProperty("payload", new Blob(Arrays.copyOfRange(data, pointer, length)));
		if (next != null) ent.setUnindexedProperty("next",next);
		datastore.put(ent);
		return ent.getKey();
	}
	
    //Factory methods:
	protected static MemoStorable load(Key key){
		byte[] result;
		try {
			Entity ent = datastore.get(key);	
			Blob blob = (Blob) ent.getProperty("payload");
			byte[] data=blob.getBytes();
			result=Arrays.copyOf(data,data.length);
			
			while (ent.hasProperty("next")){
				//Expensive, should not be used too much!
				Key next= (Key)ent.getProperty("next");
				ent = datastore.get(next);
				blob = (Blob) ent.getProperty("payload");
				data=blob.getBytes();
				result=concat(data,result); //Add to front of result, due to the storing order
			}
			
		} catch (EntityNotFoundException e) {
			e.printStackTrace();
			return null;
		}
		return _unserialize(result);
	}
	
	private static MemoStorable _unserialize(byte[] data) {
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
	
	//Tools:
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
}
