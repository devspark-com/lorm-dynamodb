package org.jstorni.lorm.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Embeddable;
import javax.persistence.Embedded;

import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.exceptions.DataValidationException;
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

public class EntityToItemMapperImpl<T> implements EntityToItemMapper,
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
	public Map<AttributeDefinition, Object> map(Object entity) {
		return map(entity, "");
	}

	private Map<AttributeDefinition, Object> map(Object entity,
			String fieldNamePrefix) {
		ReflectionSupport reflectionSupport = new ReflectionSupport();
		Map<AttributeDefinition, Object> attributes = new HashMap<AttributeDefinition, Object>();

		if (entity == null) {
			return attributes;
		}

		// it is assumed that entity will be validated before
		// mapping to Item

		List<Field> fields = reflectionSupport.getAllFields(entity.getClass());
		for (Field field : fields) {
			if (field.getAnnotation(Embedded.class) != null) {
				if (field.getType().getAnnotation(Embeddable.class) != null) {
					String embeddedFieldamePrefix = fieldNamePrefix
							+ field.getName() + ".";
					Object embedded = reflectionSupport.getValueOfField(field,
							entity);
					attributes.putAll(map(embedded, embeddedFieldamePrefix));
				} else {
					throw new DataValidationException("Error while mapping "
							+ entityClass.getName() + " .Reason: "
							+ field.getType().getName() + " is not embeddable");
				}
			} else {
				for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
					if (mappingStrategy.apply(field)) {
						mappingStrategy.map(entity, field, fieldNamePrefix,
								attributes);

						break;
					}

				}
			}
		}

		return attributes;
	}

	@Override
	public List<SchemaValidationError> validateSchema(EntitySchema entitySchema) {
		List<Class<?>> dependenciesPath = new ArrayList<Class<?>>();
		return validateSchema(entitySchema, entityClass, "", dependenciesPath);
	}

	private List<SchemaValidationError> validateSchema(
			EntitySchema entitySchema, Class<?> entityClassToParse,
			String fieldNamePrefix, List<Class<?>> dependenciesPath) {

		List<SchemaValidationError> errors = new ArrayList<SchemaValidationError>();

		ReflectionSupport reflectionSupport = new ReflectionSupport();
		List<Field> fields = reflectionSupport.getAllFields(entityClassToParse);
		for (Field field : fields) {
			if (field.getAnnotation(Embedded.class) != null) {
				if (field.getType().getAnnotation(Embeddable.class) != null) {
					// TODO move field naming to an strategy?
					String embeddedFieldamePrefix = fieldNamePrefix
							+ field.getName() + ".";
					if (dependenciesPath.contains(field.getType())) {
						errors.add(SchemaValidationError.buildRecursiveError(
								entityClass, entityClassToParse,
								field.getName()));
						continue;
					}

					List<Class<?>> embeddedDependenciesPath = new ArrayList<Class<?>>();
					embeddedDependenciesPath.addAll(dependenciesPath);
					embeddedDependenciesPath.add(field.getType());
					
					List<SchemaValidationError> currentErrors = validateSchema(
							entitySchema, field.getType(),
							embeddedFieldamePrefix, embeddedDependenciesPath);
					if (currentErrors != null && !currentErrors.isEmpty()) {
						errors.addAll(currentErrors);
					}
				} else {
					throw new DataValidationException("Error while mapping "
							+ entityClass.getName() + " .Reason: "
							+ field.getType().getName() + " is not embeddable");
				}
			} else {
				for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
					if (mappingStrategy.apply(field)) {
						List<SchemaValidationError> currentErrors = mappingStrategy
								.hasValidSchema(entitySchema, entityClass,
										field, fieldNamePrefix);
						if (currentErrors != null && !currentErrors.isEmpty()) {
							errors.addAll(currentErrors);
						}

						break;
					}
				}
			}
		}

		return errors;
	}

	@Override
	public List<AttributeDefinition> getMissingAttributesInEntityClass(
			EntitySchema entitySchema) {
		List<EntityFieldAsAttribute> entityFieldsAsAttributes = getEntityFieldsAsAttributes(
				entitySchema, entityClass, "");

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

	private List<EntityFieldAsAttribute> getEntityFieldsAsAttributes(
			EntitySchema entitySchema, Class<?> entityClassToParse,
			String fieldNamePrefix) {
		List<EntityFieldAsAttribute> entityFieldsAsAttributes = new ArrayList<EntityFieldAsAttribute>();
		ReflectionSupport reflectionSupport = new ReflectionSupport();
		List<Field> fields = reflectionSupport.getAllFields(entityClassToParse);
		for (Field field : fields) {
			if (field.getAnnotation(Embedded.class) != null) {
				if (field.getType().getAnnotation(Embeddable.class) != null) {
					String embeddedFieldamePrefix = fieldNamePrefix
							+ field.getName() + ".";
					entityFieldsAsAttributes
							.addAll(getEntityFieldsAsAttributes(entitySchema,
									field.getType(), embeddedFieldamePrefix));
				} else {
					throw new DataValidationException("Error while parsing "
							+ entityClass.getName() + " .Reason: "
							+ field.getType().getName() + " is not embeddable");

				}
			} else {
				for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
					if (mappingStrategy.apply(field)) {
						List<EntityFieldAsAttribute> fieldAsAttr = mappingStrategy
								.getEntityFieldAsAttribute(field,
										fieldNamePrefix);
						if (fieldAsAttr != null) {
							entityFieldsAsAttributes.addAll(fieldAsAttr);
						}

						break;
					}
				}
			}
		}

		return entityFieldsAsAttributes;
	}

	@Override
	public List<AttributeDefinition> getMissingFieldsInTable(
			EntitySchema entitySchema) {
		return getMissingFieldsInTable(entitySchema, entityClass, "");
	}

	private List<AttributeDefinition> getMissingFieldsInTable(
			EntitySchema entitySchema, Class<?> entityClassToParse,
			String fieldNamePrefix) {
		List<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();

		ReflectionSupport reflectionSupport = new ReflectionSupport();
		List<Field> fields = reflectionSupport.getAllFields(entityClassToParse);
		for (Field field : fields) {
			if (field.getAnnotation(Embedded.class) != null) {
				if (field.getType().getAnnotation(Embeddable.class) != null) {
					String embeddedFieldamePrefix = fieldNamePrefix
							+ field.getName() + ".";
					List<AttributeDefinition> currentAttrDefs = getMissingFieldsInTable(
							entitySchema, field.getType(),
							embeddedFieldamePrefix);
					if (currentAttrDefs != null && !currentAttrDefs.isEmpty()) {
						attrDefs.addAll(currentAttrDefs);
					}
				} else {
					throw new DataValidationException("Error while parsing "
							+ entityClass.getName() + " .Reason: "
							+ field.getType().getName() + " is not embeddable");
				}
			} else {
				for (EntityToItemMappingStrategy mappingStrategy : mappingStrategies) {
					if (mappingStrategy.apply(field)) {
						List<AttributeDefinition> currentAttrDefs = mappingStrategy
								.getSchemaUpdate(entitySchema, entityClass,
										field, fieldNamePrefix);
						if (currentAttrDefs != null
								&& !currentAttrDefs.isEmpty()) {
							attrDefs.addAll(currentAttrDefs);
						}
						break;
					}
				}
			}
		}

		return attrDefs;
	}

}
