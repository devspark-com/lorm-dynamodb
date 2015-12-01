package org.jstorni.lorm.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.mapping.strategies.entitytoitem.DateEntityToItemMappingStrategy;
import org.jstorni.lorm.mapping.strategies.entitytoitem.DefaultEntityToItemMappingStrategy;
import org.jstorni.lorm.mapping.strategies.entitytoitem.EntityToItemMappingStrategy;
import org.jstorni.lorm.mapping.strategies.entitytoitem.ManyToOneEntityToItemMappingStrategy;
import org.jstorni.lorm.mapping.strategies.entitytoitem.OneToManyEntityMappingStrategy;
import org.jstorni.lorm.mapping.strategies.entitytoitem.TransientEntityMappingStrategy;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntityFieldAsAttribute;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

public class EntityToItemMapperImpl<T> implements EntityToItemMapper<T>,
		EntitySchemaSupport {

	private final Class<T> entityClass;
	private final List<EntityToItemMappingStrategy> mappingStrategies;

	public EntityToItemMapperImpl(Class<T> entityClass) {

		super();
		this.entityClass = entityClass;

		ReflectionSupport reflectionSupport = new ReflectionSupport();
		this.mappingStrategies = new ArrayList<EntityToItemMappingStrategy>();
		mappingStrategies.add(new TransientEntityMappingStrategy());
		mappingStrategies.add(new OneToManyEntityMappingStrategy());
		mappingStrategies.add(new ManyToOneEntityToItemMappingStrategy(
				reflectionSupport));
		mappingStrategies.add(new DateEntityToItemMappingStrategy(
				reflectionSupport));
		mappingStrategies.add(new DefaultEntityToItemMappingStrategy(
				reflectionSupport));
	}

	@Override
	public Map<AttributeDefinition, Object> map(T entity) {
		ReflectionSupport reflectionSupport = new ReflectionSupport();
		Map<AttributeDefinition, Object> attributes = new HashMap<AttributeDefinition, Object>();

		// it is assumed that entity will be validated before
		// mapping to Item

		List<Field> fields = reflectionSupport.getAllFields(entityClass);
		for (Field field : fields) {
			for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
				if (mappingStrategy.apply(field)) {
					mappingStrategy.map(entity, field, attributes);
					break;
				}
			}
		}

		return attributes;
	}

	@Override
	public List<SchemaValidationError> validateSchema(EntitySchema entitySchema) {

		List<SchemaValidationError> errors = new ArrayList<SchemaValidationError>();

		ReflectionSupport reflectionSupport = new ReflectionSupport();
		List<Field> fields = reflectionSupport.getAllFields(entityClass);
		for (Field field : fields) {
			for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
				if (mappingStrategy.apply(field)) {
					SchemaValidationError error = mappingStrategy
							.hasValidSchema(entitySchema, entityClass, field);
					if (error != null) {
						errors.add(error);
					}

					break;
				}
			}
		}

		return errors;
	}

	@Override
	public List<AttributeDefinition> getMissingAttributesInEntityClass(
			EntitySchema entitySchema) {
		List<EntityFieldAsAttribute> entityFieldsAsAttributes = new ArrayList<EntityFieldAsAttribute>();

		ReflectionSupport reflectionSupport = new ReflectionSupport();
		List<Field> fields = reflectionSupport.getAllFields(entityClass);
		for (Field field : fields) {
			for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
				if (mappingStrategy.apply(field)) {
					EntityFieldAsAttribute fieldAsAttr = mappingStrategy
							.getEntityFieldAsAttribute(field);
					if (fieldAsAttr != null) {
						entityFieldsAsAttributes.add(fieldAsAttr);
					}

					break;
				}
			}
		}

		List<AttributeDefinition> missingAttrDefs = new ArrayList<AttributeDefinition>();

		Set<AttributeDefinition> attrDefs = entitySchema.getAttributes();
		for (AttributeDefinition attributeDefinition : attrDefs) {
			boolean found = false;
			for (EntityFieldAsAttribute entityFieldAsAttr : entityFieldsAsAttributes) {
				if (entityFieldAsAttr.getAttributeName().equals(
						attributeDefinition.getName())) {
					found = true;
					break;
				}
			}

			if (!found) {
				missingAttrDefs.add(attributeDefinition);
			}
		}

		return missingAttrDefs;
	}

	@Override
	public List<AttributeDefinition> getMissingFieldsInTable(
			EntitySchema entitySchema) {
		List<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();

		ReflectionSupport reflectionSupport = new ReflectionSupport();
		List<Field> fields = reflectionSupport.getAllFields(entityClass);
		for (Field field : fields) {
			for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
				if (mappingStrategy.apply(field)) {
					AttributeDefinition attrDef = mappingStrategy
							.getSchemaUpdate(entitySchema, entityClass, field);
					if (attrDef != null) {
						attrDefs.add(attrDef);
					}
					break;
				}
			}
		}

		return attrDefs;
	}

}
