package org.jstorni.lorm.mapping.strategies.itemtoentity;

import java.lang.reflect.Field;
import java.util.Map.Entry;

import org.jstorni.lorm.schema.AttributeDefinition;

public interface ItemToEntityMappingStrategy {

	boolean apply(Field field);

	void map(Entry<AttributeDefinition, Object> itemEntry, Field field,
			Object entityInstance);

}
