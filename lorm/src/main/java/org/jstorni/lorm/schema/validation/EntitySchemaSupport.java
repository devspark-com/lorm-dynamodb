package org.jstorni.lorm.schema.validation;

import java.util.List;

import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.EntitySchema;

public interface EntitySchemaSupport {

	List<SchemaValidationError> validateSchema(EntitySchema entitySchema);

	List<AttributeDefinition> getMissingFieldsInTable(EntitySchema entitySchema);

	List<AttributeDefinition> getMissingAttributesInEntityClass(
			EntitySchema entitySchema);

	// TODO get missing indexes in entity

	// TODO get missing indexes in table

}
