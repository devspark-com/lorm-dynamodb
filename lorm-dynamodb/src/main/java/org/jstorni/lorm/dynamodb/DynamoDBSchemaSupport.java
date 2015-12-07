package org.jstorni.lorm.dynamodb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.persistence.Entity;

import org.jstorni.lorm.SchemaSupport;
import org.jstorni.lorm.exceptions.DataException;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.id.EntityIdHandler;
import org.jstorni.lorm.schema.AttributeConstraint;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.AttributeType;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class DynamoDBSchemaSupport<T> implements SchemaSupport<T> {

	private final EntityIdHandler idHandler;
	private Table table;
	private final DynamoDB dynamoDB;
	private final Class<?> entityClass;
	private final EntitySchemaSupport entitySchemaSupport;

	
	public DynamoDBSchemaSupport(DynamoDB dynamoDB,
			EntitySchemaSupport entitySchemaSupport, 
			Class<?> entityClass) {
		idHandler = new EntityIdHandler(entityClass);
		this.dynamoDB = dynamoDB;
		this.entitySchemaSupport = entitySchemaSupport;
		this.entityClass = entityClass;
		table = getDynamoDbTable();
	}

	private Table getDynamoDbTable() {
		Entity entityAnnotation = entityClass.getAnnotation(Entity.class);
		if (entityAnnotation == null) {
			throw new DataException("Not an entity: " + entityClass.getName());
		}

		String tableName = entityAnnotation.name() != null
				&& !entityAnnotation.name().isEmpty() ? entityAnnotation.name()
				: entityClass.getName()
						.substring(entityClass.getName().lastIndexOf('.') + 1)
						.toLowerCase();

		return dynamoDB.getTable(tableName);
	}

	protected Table getTable() {
		return table;
	}

	protected DynamoDB getDynamoDB() {
		return dynamoDB;
	}

	protected EntityIdHandler getIdHandler() {
		return idHandler;
	}
	
	protected Class<?> getEntityClass() {
		return entityClass;
	}
	
	
	@Override
	public boolean isValid(final List<SchemaValidationError> errors) {

		if (getTable().getDescription() == null) {
			try {
				getTable().describe();
			} catch (ResourceNotFoundException ex) {
				errors.add(SchemaValidationError.buildTableError(getTable()
						.getTableName(), "Table not found: "
						+ getTable().getTableName()));
				return false;
			}
		}

		List<SchemaValidationError> validationErrors = entitySchemaSupport
				.validateSchema(getMapToEntitySchema());
		validationErrors.stream()
				.filter(new Predicate<SchemaValidationError>() {

					@Override
					public boolean test(SchemaValidationError error) {
						Set<AttributeConstraint> attrConstraints = error
								.getAttributeDefinition().getConstraints();
						return attrConstraints != null
								&& attrConstraints.contains(AttributeConstraint
										.buildPrimaryKeyConstraint());
					}
				}).forEach(new Consumer<SchemaValidationError>() {

					@Override
					public void accept(SchemaValidationError error) {
						errors.add(error);
					}
				});

		return errors.isEmpty();
	}

	private EntitySchema getMapToEntitySchema() {
		Set<AttributeDefinition> attributes = new HashSet<AttributeDefinition>();

		TableDescription tableDesc = getTable().getDescription();
		List<com.amazonaws.services.dynamodbv2.model.AttributeDefinition> sourceAttrs = tableDesc
				.getAttributeDefinitions();
		for (com.amazonaws.services.dynamodbv2.model.AttributeDefinition attributeDefinition : sourceAttrs) {
			AttributeType attrType = getAttributeType(attributeDefinition
					.getAttributeType());

			// TODO get constraints
			attributes.add(new AttributeDefinition(attributeDefinition
					.getAttributeName(), attrType, null));
		}

		return new EntitySchema(tableDesc.getTableName(), attributes);
	}

	private AttributeType getAttributeType(String scalarAttributeType) {
		AttributeType attrType;
		if (scalarAttributeType.equals(ScalarAttributeType.S.toString())) {
			attrType = AttributeType.STRING;
		} else if (scalarAttributeType.equals(ScalarAttributeType.N.toString())) {
			attrType = AttributeType.NUMBER;
		} else if (scalarAttributeType.equals(ScalarAttributeType.B.toString())) {
			attrType = AttributeType.BOOLEAN;
		} else {
			throw new DataValidationException(
					"Unsupported Dynamo attribute type: " + scalarAttributeType);
		}

		return attrType;

	}

	protected AttributeType getAttributeType(Class<?> attributeClass) {
		// TODO add other supported types
		AttributeType attrType;
		if (Number.class.isAssignableFrom(attributeClass)) {
			attrType = AttributeType.NUMBER;
		} else if (String.class.equals(attributeClass)) {
			attrType = AttributeType.STRING;
		} else if (Boolean.class.equals(attributeClass)) {
			attrType = AttributeType.BOOLEAN;
		} else {
			throw new DataValidationException("Attribute type not supported: "
					+ attributeClass.getName());
		}

		return attrType;

	}

	@Override
	public boolean syncToSchema(boolean deleteMissingFields,
			boolean createTableIfNotExists, boolean createReferences) {

		boolean isNewTable = false;

		try {
			getTable().describe();
		} catch (ResourceNotFoundException ex) {
			if (createTableIfNotExists) {
				isNewTable = true;
			} else {
				throw new DataValidationException("Table not found: "
						+ getTable().getTableName());
			}
		}

		if (createTableIfNotExists && isNewTable) {
			List<com.amazonaws.services.dynamodbv2.model.AttributeDefinition> attrDefs = new ArrayList<com.amazonaws.services.dynamodbv2.model.AttributeDefinition>();
			// TODO assuming String id
			attrDefs.add(new com.amazonaws.services.dynamodbv2.model.AttributeDefinition(
					idHandler.getIdFieldName(), ScalarAttributeType.S));

			List<KeySchemaElement> keys = new ArrayList<KeySchemaElement>();
			keys.add(new KeySchemaElement(idHandler.getIdFieldName(),
					KeyType.HASH));

			// TODO get this from configuration
			ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
			provisionedThroughput.setReadCapacityUnits(1L);
			provisionedThroughput.setWriteCapacityUnits(1L);

			CreateTableRequest createTableReq = new CreateTableRequest()
					.withTableName(getTable().getTableName()).withKeySchema(
							keys);
			createTableReq.setAttributeDefinitions(attrDefs);
			createTableReq.setProvisionedThroughput(provisionedThroughput);

			table = dynamoDB.createTable(createTableReq);
			try {
				table.waitForActive();
			} catch (InterruptedException ex) {
				throw new DataValidationException(
						"Error while creating table: "
								+ getTable().getTableName(), ex);
			}
		}

		if (!isNewTable) {
			List<SchemaValidationError> errors = new ArrayList<SchemaValidationError>();
			if (!isValid(errors)) {
				// TODO support secondary index creation

				throw new DataValidationException("Table "
						+ getTable().getTableName() + " is invalid");
			}
		}

		return true;
	}

}
