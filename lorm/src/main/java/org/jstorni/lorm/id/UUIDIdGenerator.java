package org.jstorni.lorm.id;

import java.util.UUID;

public class UUIDIdGenerator implements IdGenerator {

	public UUIDIdGenerator() {
	}
	
	@Override
	public String generateId() {
		return UUID.randomUUID().toString();
	}

}
