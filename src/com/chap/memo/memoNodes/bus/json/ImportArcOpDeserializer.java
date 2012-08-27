package com.chap.memo.memoNodes.bus.json;

import java.io.IOException;

import com.chap.memo.memoNodes.model.ArcOp;
import com.chap.memo.memoNodes.model.OpsType;
import com.eaio.uuid.UUID;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class ImportArcOpDeserializer extends StdDeserializer<ArcOp> {
	
	public ImportArcOpDeserializer() {
		super(ArcOp.class);
	}

	@Override
	public ArcOp deserialize(JsonParser jp, DeserializationContext context)
			throws IOException, JsonProcessingException {

		OpsType type = null;
		UUID parent=null;
		UUID child=null;
		long timestamp=0;
		boolean skip=false;
		
		JsonToken token = jp.nextToken();
		if (token != JsonToken.START_OBJECT) {
		    skip=true;
		}
		while (skip || (token = jp.nextToken()) != JsonToken.END_OBJECT) {
			skip=false;
			String fieldName = jp.getCurrentName();
			jp.nextToken();
			if (fieldName.equals("type")){
				type=OpsType.valueOf(jp.getText());
			}
			if (fieldName.equals("parentString")){
				parent=new UUID(jp.getText());
			}
			if (fieldName.equals("childString")){
				child=new UUID(jp.getText());
			}
			if (fieldName.equals("timestamp_long")){
				timestamp=jp.getLongValue();
			}
		}
		try {
			return new ArcOp(type,parent,child,timestamp);
		} catch (Exception e){
			throw new IOException("Something went wrong deserializing nodeValues:"+e.getLocalizedMessage());
		}
	}
}
