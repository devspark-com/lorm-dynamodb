package com.jstorni.core.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.persistence.Entity;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.jstorni.core.dynamodb.exceptions.DataException;
import com.jstorni.core.dynamodb.exceptions.DataValidationException;
import com.jstorni.core.dynamodb.id.EntityIdHandler;
import com.jstorni.core.dynamodb.mapping.EntityToItemMapper;
import com.jstorni.core.dynamodb.mapping.ItemToEntityMapper;
import com.jstorni.core.dynamodb.schema.AttributeConstraint;
import com.jstorni.core.dynamodb.schema.AttributeDefinition;
import com.jstorni.core.dynamodb.schema.AttributeType;
import com.jstorni.core.dynamodb.schema.EntitySchema;
import com.jstorni.core.dynamodb.schema.validation.EntitySchemaSupport;
import com.jstorni.core.dynamodb.schema.validation.SchemaValidationError;

// TODO log (not AWS logger) computing per query, specially when fetching references
public class DynamoDBBaseRepository<T> implements Repository<T>,
		SchemaSupport<T> {

	private final EntityIdHandler<T> idHandler;
	private Table table;
	private final DynamoDB dynamoDB;
	private final Class<?> entityClass;
	private final EntityToItemMapper<T> entityToItemMapper;
	private final ItemToEntityMapper<T> itemToEntityMapper;
	private final EntitySchemaSupport entitySchemaSupport;

	public DynamoDBBaseRepository(DynamoDB dynamoDB,
			EntityToItemMapper<T> entityToItemMapper,
			ItemToEntityMapper<T> itemToEntityMapper,
			EntitySchemaSupport entitySchemaSupport, Class<?> entityClass) {
		idHandler = new EntityIdHandler<T>(entityClass);
		this.dynamoDB = dynamoDB;
		this.entityToItemMapper = entityToItemMapper;
		this.itemToEntityMapper = itemToEntityMapper;
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.jstorni.core.dynamodb.Repository#findOne(java.lang.String)
	 */
	@Override
	public T findOne(String id) {
		Item item = table.getItem(idHandler.buildPrimaryKey(id));

		return itemToEntityMapper.map(extractAttrsFromItem(item));
	}

	private Map<AttributeDefinition, Object> extractAttrsFromItem(Item item) {
		Map<AttributeDefinition, Object> attributes = new HashMap<AttributeDefinition, Object>();

		Map<String, Object> itemMap = item.asMap();
		for (String attrName : itemMap.keySet()) {
			Object itemValue = item.get(attrName);
			attributes.put(new AttributeDefinition(attrName,
					getAttributeType(itemValue.getClass()), null), itemValue);
		}

		return attributes;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.jstorni.core.dynamodb.Repository#findAll()
	 */
	@Override
	public List<T> findAll() {
		// TODO set a default output limit
		// TODO add page support
		ItemCollection<ScanOutcome> scannedItems = table.scan();
		List<T> items = new ArrayList<T>();

		IteratorSupport<Item, ScanOutcome> scannedItemsIt = scannedItems
				.iterator();
		Item item;
		while ((item = scannedItemsIt.next()) != null) {
			items.add(itemToEntityMapper.map(extractAttrsFromItem(item)));
		}

		return items;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.jstorni.core.dynamodb.Repository#save(T)
	 */
	@Override
	public T save(T instance) {
		String id = idHandler.getIdValue(instance);
		if (id == null) {
			String generatedId = idHandler.generateId(instance);
			if (generatedId == null) {
				throw new DataValidationException(
						"Entity id should not be null");
			}

			idHandler.setIdValue(instance, generatedId);
		}

		Map<AttributeDefinition, Object> attributes = entityToItemMapper
				.map(instance);

		Item item = buildItem(attributes);
		// TODO execute @PrePersist

		table.putItem(item);

		// TODO execute @PostPersist

		return instance;
	}

	private Item buildItem(Map<AttributeDefinition, Object> attributes) {
		Item item = new Item();

		for (AttributeDefinition attrDef : attributes.keySet()) {
			if (attrDef.getConstraints() != null) {
				if (attrDef.getConstraints().contains(
						AttributeConstraint.buildPrimaryKeyConstraint())) {
					item.withPrimaryKey(attrDef.getName(),
							attributes.get(attrDef));
				}
			}

			item.with(attrDef.getName(), attributes.get(attrDef));
		}

		return item;
	}

	@Override
	public List<T> save(List<T> instances) {
		for (T instance : instances) {
			save(instance);
		}

		return instances;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.jstorni.core.dynamodb.Repository#deleteById(java.lang.String)
	 */
	@Override
	public void deleteById(String id) {
		table.deleteItem(idHandler.buildPrimaryKey(id));
	}

	@Override
	public void deleteById(List<String> ids) {
		for (String id : ids) {
			deleteById(id);
		}
	}

	@Override
	public boolean isValid(List<SchemaValidationError> errors) {

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

	private ScalarAttributeType getAttributeType(AttributeType attributeType) {
		ScalarAttributeType scalarAttrType;
		if (attributeType.equals(AttributeType.STRING)) {
			scalarAttrType = ScalarAttributeType.S;
		} else if (attributeType.equals(AttributeType.NUMBER)) {
			scalarAttrType = ScalarAttributeType.N;
		} else if (attributeType.equals(AttributeType.BOOLEAN)) {
			scalarAttrType = ScalarAttributeType.B;
		} else {
			throw new DataValidationException("Unsupported attribute type: "
					+ attributeType.toString());
		}

		return scalarAttrType;

	}

	private AttributeType getAttributeType(Class<?> attributeClass) {
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
