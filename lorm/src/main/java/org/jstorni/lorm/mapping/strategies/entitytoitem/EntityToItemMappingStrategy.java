package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
import java.util.Map;

import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntityFieldAsAttribute;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

public interface EntityToItemMappingStrategy {

	SchemaValidationError hasValidSchema(EntitySchema entitySchema,
			Class<?> entityClass, Field field);

	AttributeDefinition getSchemaUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field);

	EntityFieldAsAttribute getEntityFieldAsAttribute(Field field);

	boolean apply(Field field);

	void map(Object entity, Field field, Map<AttributeDefinition, Object> attributes);
}
