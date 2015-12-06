package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import javax.persistence.Transient;

import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntityFieldAsAttribute;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

public class TransientEntityMappingStrategy implements
		EntityToItemMappingStrategy {

	@Override
	public boolean apply(Field field) {
		return field.getAnnotation(Transient.class) != null;
	}

	@Override
	public List<EntityFieldAsAttribute> getEntityFieldAsAttribute(Field field,
			String fieldNamePrefix) {
		return null;
	}

	@Override
	public List<AttributeDefinition> getSchemaUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field, String fieldNamePrefix) {
		return null;
	}

	@Override
	public List<SchemaValidationError> hasValidSchema(
			EntitySchema entitySchema, Class<?> entityClass, Field field,
			String fieldNamePrefix) {
		return null;
	}

	@Override
	public void map(Object entity, Field field, String fieldNamePrefix,
			Map<AttributeDefinition, Object> attributes) {
	}
}
