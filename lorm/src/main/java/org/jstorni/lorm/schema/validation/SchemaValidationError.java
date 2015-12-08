package org.jstorni.lorm.schema.validation;

import org.jstorni.lorm.schema.AttributeDefinition;

public class SchemaValidationError {
	private final AttributeDefinition attributeDefinition;
	private final SchemaValidationErrorType errorType;
	private final String message;
	private final String identifier;
	private final SchemaValidationScope validationScope;

	public SchemaValidationError(AttributeDefinition attributeDefinition,
			SchemaValidationErrorType errorType,
			SchemaValidationScope validationScope, String identifier,
			String message) {
		super();
		this.attributeDefinition = attributeDefinition;
		this.errorType = errorType;
		this.message = message;
		this.identifier = identifier;
		this.validationScope = validationScope;
	}

	public static SchemaValidationError buildTableError(String tableName,
			String message) {
		return new SchemaValidationError(null,
				SchemaValidationErrorType.GENERAL, SchemaValidationScope.TABLE,
				tableName, message);
	}

	public static SchemaValidationError buildGeneralError(
			AttributeDefinition attributeDefinition, String message) {
		return new SchemaValidationError(attributeDefinition,
				SchemaValidationErrorType.GENERAL,
				SchemaValidationScope.ATTRIBUTE, attributeDefinition.getName(),
				message);
	}

	public static SchemaValidationError buildMissingFieldError(
			AttributeDefinition attributeDefinition) {
		String message = "Missing attribute in table: "
				+ attributeDefinition.getName();
		return new SchemaValidationError(attributeDefinition,
				SchemaValidationErrorType.MISSING_IN_TABLE,
				SchemaValidationScope.ATTRIBUTE, attributeDefinition.getName(),
				message);
	}

	public static SchemaValidationError buildWrongFieldTypeError(
			AttributeDefinition attributeDefinition, String message) {
		return new SchemaValidationError(attributeDefinition,
				SchemaValidationErrorType.MISSING_IN_TABLE,
				SchemaValidationScope.ATTRIBUTE, attributeDefinition.getName(),
				message);
	}

	public static SchemaValidationError buildMissingAttributeError(
			AttributeDefinition attributeDefinition) {
		String message = "Missing attribute in entity: "
				+ attributeDefinition.getName();
		return new SchemaValidationError(attributeDefinition,
				SchemaValidationErrorType.MISSING_IN_ENTITY,
				SchemaValidationScope.ATTRIBUTE, attributeDefinition.getName(),
				message);
	}

	public static SchemaValidationError buildNotNeededFieldError(
			AttributeDefinition attributeDefinition, String message) {
		return new SchemaValidationError(attributeDefinition,
				SchemaValidationErrorType.MISSING_IN_ENTITY,
				SchemaValidationScope.ATTRIBUTE, attributeDefinition.getName(),
				message);
	}

	public static SchemaValidationError buildRecursiveError(
			Class<?> entityClass, Class<?> recursiveClass,
			String attributeName) {
		return new SchemaValidationError(null,
				SchemaValidationErrorType.GENERAL,
				SchemaValidationScope.ATTRIBUTE, attributeName,
				"Found recursive dependency path on entity "
						+ entityClass.getName() + ", with dependency type "
						+ recursiveClass.getName());
	}

	public AttributeDefinition getAttributeDefinition() {
		return attributeDefinition;
	}

	public SchemaValidationErrorType getErrorType() {
		return errorType;
	}

	public String getMessage() {
		return message;
	}

	public String getIdentifier() {
		return identifier;
	}

	public SchemaValidationScope getValidationScope() {
		return validationScope;
	}

}
