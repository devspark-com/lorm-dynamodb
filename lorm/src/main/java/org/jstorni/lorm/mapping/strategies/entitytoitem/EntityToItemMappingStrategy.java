package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntityFieldAsAttribute;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

public interface EntityToItemMappingStrategy {

	List<SchemaValidationError> hasValidSchema(EntitySchema entitySchema,
			Class<?> entityClass, Field field, String fieldNamePrefix);

	List<AttributeDefinition> getSchemaUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field, String fieldNamePrefix);

	List<EntityFieldAsAttribute> getEntityFieldAsAttribute(Field field, String fieldNamePrefix);

	boolean apply(Field field);

	void map(Object entity, Field field, String fieldNamePrefix,
			Map<AttributeDefinition, Object> attributes);
}
