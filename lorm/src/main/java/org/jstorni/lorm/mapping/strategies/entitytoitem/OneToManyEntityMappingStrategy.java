package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
import java.util.Map;

import javax.persistence.OneToMany;

import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntityFieldAsAttribute;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

public class OneToManyEntityMappingStrategy implements
		EntityToItemMappingStrategy {

	@Override
	public SchemaValidationError hasValidSchema(EntitySchema entitySchema,
			Class<?> entityClass, Field field) {
		throw new DataValidationException(
				"OneToMany annotation is not supported");
	}

	@Override
	public AttributeDefinition getSchemaUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field) {
		throw new DataValidationException(
				"OneToMany annotation is not supported");
	}

	@Override
	public EntityFieldAsAttribute getEntityFieldAsAttribute(Field field) {
		throw new DataValidationException(
				"OneToMany annotation is not supported");
	}

	@Override
	public boolean apply(Field field) {
		return field.getAnnotation(OneToMany.class) != null;
	}

	@Override
	public void map(Object entity, Field field,
			Map<AttributeDefinition, Object> attributes) {
		throw new DataValidationException(
				"OneToMany annotation is not supported");

	}

}
