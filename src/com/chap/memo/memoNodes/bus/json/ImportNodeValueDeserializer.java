package com.chap.memo.memoNodes.bus.json;

import java.io.IOException;

import com.chap.memo.memoNodes.model.NodeValue;
import com.eaio.uuid.UUID;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class ImportNodeValueDeserializer extends StdDeserializer<NodeValue> {

	public ImportNodeValueDeserializer() {
		super(NodeValue.class);
	}

	@Override
	public NodeValue deserialize(JsonParser jp, DeserializationContext context)
			throws IOException, JsonProcessingException {
		byte[] value=null;
		long timestamp=0;
		UUID uuid=null;
		boolean skip=false;
		
		JsonToken token = jp.nextToken();
		if (token != JsonToken.START_OBJECT) {
		    skip=true;
		}
		while (skip || (token = jp.nextToken()) != JsonToken.END_OBJECT) {
			skip=false;
			String fieldName = jp.getCurrentName();
			jp.nextToken();
			if (fieldName.equals("value")){
				value=jp.getBinaryValue();
			}
			if (fieldName.equals("idString")){
				uuid=new UUID(jp.getText());
			}
			if (fieldName.equals("timestamp_long")){
				timestamp=jp.getLongValue();
			}
		}
		try {
			return new NodeValue(uuid,value,timestamp);
		} catch (Exception e){
			throw new IOException("Something went wrong deserializing nodeValues:"+e.getLocalizedMessage());
		}
	}

}
