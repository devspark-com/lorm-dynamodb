package org.jstorni.lorm.mapping.strategies.itemtoentity;

import java.lang.reflect.Field;
import java.util.Map.Entry;

import javax.persistence.Transient;

import org.jstorni.lorm.schema.AttributeDefinition;

public class TransientItemToEntityMappingStrategy implements
		ItemToEntityMappingStrategy {

	@Override
	public boolean apply(Field field) {
		return field.getAnnotation(Transient.class) != null;
	}

	@Override
	public void map(Entry<AttributeDefinition, Object> itemEntry, Field field,
			Object entityInstance) {
	}

}
