package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Id;

import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.schema.AttributeConstraint;
import org.jstorni.lorm.schema.AttributeConstraintType;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.AttributeType;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntityFieldAsAttribute;
import org.jstorni.lorm.schema.validation.SchemaValidationError;
import org.jstorni.lorm.schema.validation.SchemaValidationErrorType;

// TODO consider also Document and Set types
public class DefaultEntityToItemMappingStrategy implements
		EntityToItemMappingStrategy {

	protected final ReflectionSupport reflectionSupport;

	public DefaultEntityToItemMappingStrategy(
			ReflectionSupport reflectionSupport) {
		super();
		this.reflectionSupport = reflectionSupport;
	}

	@Override
	public List<AttributeDefinition> getSchemaUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field, String fieldNamePrefix) {
		if (!needsUpdate(entitySchema, entityClass, field, fieldNamePrefix)) {
			return null;
		}

		List<AttributeDefinition> attrs = new ArrayList<AttributeDefinition>();
		attrs.add(buildAttributeDefinition(field, fieldNamePrefix));
		
		return attrs;
	}

	protected AttributeDefinition buildAttributeDefinition(Field field, String fieldNamePrefix) {
		String attrName = fieldNamePrefix + field.getName();
		AttributeType attrType;
		if (field.getType().equals(Boolean.class)) {
			attrType = AttributeType.BOOLEAN;
		} else if (field.getType().equals(String.class)) {
			attrType = AttributeType.STRING;
		} else if (Number.class.isAssignableFrom(field.getType())) {
			attrType = AttributeType.NUMBER;
		} else {
			throw new DataValidationException(
					"Schema update not supported for type: "
							+ field.getType().getName());
		}

		// TODO support constraints
		Set<AttributeConstraint> constraints = new HashSet<AttributeConstraint>();
		if (field.getAnnotation(Id.class) != null) {
			constraints.add(new AttributeConstraint(
					AttributeConstraintType.PRIMARY_KEY, null));
		}

		return new AttributeDefinition(attrName, attrType, constraints);

	}

	protected boolean needsUpdate(EntitySchema entitySchema,
			Class<?> entityClass, Field field, String fieldNamePrefix) {
		List<SchemaValidationError> validationErrors = hasValidSchema(entitySchema,
				entityClass, field, fieldNamePrefix);

		if (validationErrors == null || validationErrors.isEmpty()) {
			return false; // nothing to do
		}

		for (SchemaValidationError schemaValidationError : validationErrors) {
			if (schemaValidationError.getErrorType().equals(
					SchemaValidationErrorType.WRONG_TYPE)) {
				throw new DataValidationException("Attribute " + field.getName()
						+ " already exists in table " + entitySchema.getName()
						+ " but with different data type. Details: "
						+ schemaValidationError.getMessage());
			} else if (schemaValidationError.getErrorType().equals(
					SchemaValidationErrorType.GENERAL)) {
				throw new DataValidationException(
						"Error while pushing attributes to table. Details: "
								+ schemaValidationError.getMessage());
			}
		}

		// missing field

		return true;
	}

	@Override
	public List<SchemaValidationError> hasValidSchema(EntitySchema entitySchema,
			Class<?> entityClass, Field field, String fieldNamePrefix) {

		AttributeDefinition attrDef = buildAttributeDefinition(field, fieldNamePrefix);

		List<SchemaValidationError> errors = new ArrayList<SchemaValidationError>();
		if (entitySchema == null) {
			errors.add(SchemaValidationError.buildGeneralError(attrDef,
					"Table description not available"));
			return errors;
		}

		SchemaValidationError error = null;
		boolean found = false;

		Set<AttributeDefinition> attrDefs = entitySchema.getAttributes();
		for (AttributeDefinition attributeDefinition : attrDefs) {
			if (attributeDefinition.getName().equals(attrDef.getName())) {
				if (!checkAttribute(field, attributeDefinition.getType())) {
					String message = "Invalid attribute type. Field data type: "
							+ field.getType().getName()
							+ " .Schema data type: "
							+ attributeDefinition.getType();
					error = SchemaValidationError.buildWrongFieldTypeError(
							attributeDefinition, message);
				}
				found = true;
				break;
			}
		}

		if (!found) {
			error = SchemaValidationError.buildMissingFieldError(attrDef);
		}

		if (error != null) {
			errors.add(error);	
		}
		
		return errors;
	}

	protected boolean checkAttribute(Field field, AttributeType attrType) {
		final boolean valid;
		if (attrType.equals(AttributeType.BOOLEAN)) {
			valid = field.getType().equals(Boolean.class);
		} else if (attrType.equals(AttributeType.STRING)) {
			valid = field.getType().equals(String.class);
		} else if (attrType.equals(AttributeType.NUMBER)) {
			valid = Number.class.isAssignableFrom(field.getType());
		} else {
			valid = false;
		}

		return valid;
	}

	@Override
	public boolean apply(Field field) {
		return true;
	}

	@Override
	public void map(Object entity, Field field, String fieldNamePrefix,
			Map<AttributeDefinition, Object> attributes) {
		attributes.put(buildAttributeDefinition(field, fieldNamePrefix),
				reflectionSupport.getValueOfField(field, entity));
	}

	@Override
	public List<EntityFieldAsAttribute> getEntityFieldAsAttribute(Field field,
			String fieldNamePrefix) {
		List<EntityFieldAsAttribute> attrs = new ArrayList<EntityFieldAsAttribute>();
		attrs.add(new EntityFieldAsAttribute(field.getType(), fieldNamePrefix + field.getName()));

		return attrs;
	}

}
