package org.jstorni.lorm.mapping;

import java.util.Map;

import org.jstorni.lorm.schema.AttributeDefinition;

public interface ItemToEntityMapper <E> {
	
	E map(Map<AttributeDefinition, Object> attributes);
	
}
