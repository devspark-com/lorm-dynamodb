package org.jstorni.lorm.mapping.strategies.itemtoentity;

import java.lang.reflect.Field;
import java.util.Map.Entry;

import javax.persistence.OneToMany;

import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.schema.AttributeDefinition;

public class OneToManyItemToEntityMappingStrategy implements
		ItemToEntityMappingStrategy {

	@Override
	public boolean apply(Field field) {
		return field.getAnnotation(OneToMany.class) != null;
	}

	@Override
	public void map(Entry<AttributeDefinition, Object> itemEntry, Field field,
			Object entityInstance) {
		throw new DataValidationException(
				"OneToMany annotation is not supported");
	}

}
