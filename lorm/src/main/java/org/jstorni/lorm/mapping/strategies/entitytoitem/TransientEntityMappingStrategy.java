package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
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
	public EntityFieldAsAttribute getEntityFieldAsAttribute(Field field) {
		return null;
	}

	@Override
	public AttributeDefinition getSchemaUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field) {
		return null;
	}

	@Override
	public SchemaValidationError hasValidSchema(EntitySchema entitySchema,
			Class<?> entityClass, Field field) {
		return null;
	}

	@Override
	public void map(Object entity, Field field,
			Map<AttributeDefinition, Object> attributes) {
	}
}
