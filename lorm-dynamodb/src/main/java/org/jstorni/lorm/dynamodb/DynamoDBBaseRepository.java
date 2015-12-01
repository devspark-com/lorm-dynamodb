package org.jstorni.lorm.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jstorni.lorm.Repository;
import org.jstorni.lorm.SchemaSupport;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.mapping.EntityToItemMapper;
import org.jstorni.lorm.mapping.ItemToEntityMapper;
import org.jstorni.lorm.schema.AttributeConstraint;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

// TODO log (not AWS logger) computing per query, specially when fetching references
public class DynamoDBBaseRepository<T> extends DynamoDBSchemaSupport<T>
		implements Repository<T>, SchemaSupport<T> {

	private final EntityToItemMapper<T> entityToItemMapper;
	private final ItemToEntityMapper<T> itemToEntityMapper;

	protected final Log log = LogFactory.getLog(getClass());

	protected final static int BATCH_WRITE_ITEMS_LIMIT = 25;
	protected final static int BATCH_DELETE_ITEMS_LIMIT = 25;
	
	public DynamoDBBaseRepository(DynamoDB dynamoDB,
			EntityToItemMapper<T> entityToItemMapper,
			ItemToEntityMapper<T> itemToEntityMapper,
			EntitySchemaSupport entitySchemaSupport, Class<?> entityClass) {
		super(dynamoDB, entitySchemaSupport, entityClass);
		this.entityToItemMapper = entityToItemMapper;
		this.itemToEntityMapper = itemToEntityMapper;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.jstorni.core.dynamodb.Repository#findOne(java.lang.String)
	 */
	@Override
	public T findOne(String id) {
		Item item = getTable().getItem(buildPrimaryKey(id));

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
		ItemCollection<ScanOutcome> scannedItems = getTable().scan();
		List<T> items = new ArrayList<T>();

		IteratorSupport<Item, ScanOutcome> scannedItemsIt = scannedItems
				.iterator();
		Item item;
		try {
			while ((item = scannedItemsIt.next()) != null) {
				items.add(itemToEntityMapper.map(extractAttrsFromItem(item)));
			}
		} catch (NoSuchElementException ex) {
			// ignore
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
		Item item = buildItem(instance);

		// TODO execute @PrePersist

		getTable().putItem(item);

		// TODO execute @PostPersist

		return instance;
	}

	private Item buildItem(T instance) {
		String id = getIdHandler().getIdValue(instance);
		if (id == null) {
			String generatedId = getIdHandler().generateId(instance);
			if (generatedId == null) {
				throw new DataValidationException(
						"Entity id should not be null");
			}

			getIdHandler().setIdValue(instance, generatedId);
		}

		Map<AttributeDefinition, Object> attributes = entityToItemMapper
				.map(instance);

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

		List<Item> items = new ArrayList<Item>();
		for (T instance : instances) {
			boolean newItem = false;
			if (getIdHandler().getIdValue(instance) == null) {
				newItem = true;
			}

			Item item = buildItem(instance);
			if (newItem) {
				// batch write is just for existing items
				doSingleInsert(item);
				
			} else {
				// TODO execute @PrePersist
				items.add(item);
				if (items.size() == BATCH_WRITE_ITEMS_LIMIT) {
					doBatchUpdateOrDelete(new TableWriteItems(getTable()
							.getTableName()).withItemsToPut(items));
					
					// TODO execute @PostPersist

					items.clear();
				}
			}
		}

		if (!items.isEmpty()) {
			doBatchUpdateOrDelete(new TableWriteItems(getTable().getTableName())
					.withItemsToPut(items));
			// TODO execute @PostPersist
		}

		return instances;
	}

	private void doSingleInsert(Item item) {
		getTable().putItem(item);
	}
	
	private void doBatchUpdateOrDelete(TableWriteItems tableWriteItems) {
		BatchWriteItemOutcome outcome = getDynamoDB()
				.batchWriteItem(tableWriteItems);

		// keep trying until items are fully processed
		// TODO search for exponential backoff and timeout
		int tryCount = 0;
		do {
			Map<String, List<WriteRequest>> unprocessedItems = outcome
					.getUnprocessedItems();

			if (outcome.getUnprocessedItems().size() > 0) {
				log.warn("Going to retry to update/delete "
						+ outcome.getUnprocessedItems().size()
						+ " unprocessed items (try #" + (++tryCount) + ")");
				outcome = getDynamoDB().batchWriteItemUnprocessed(unprocessedItems);
			}

		} while (outcome.getUnprocessedItems().size() > 0);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.jstorni.core.dynamodb.Repository#deleteById(java.lang.String)
	 */
	@Override
	public void deleteById(String id) {
		getTable().deleteItem(buildPrimaryKey(id));
	}

	@Override
	public void deleteById(List<String> ids) {
		List<String> idsToDelete = new ArrayList<String>();

		String idFieldName = getIdHandler().getIdFieldName();

		for (String id : ids) {
			idsToDelete.add(id);

			if (idsToDelete.size() == BATCH_DELETE_ITEMS_LIMIT) {
				doBatchUpdateOrDelete(new TableWriteItems(getTable()
						.getTableName()).withHashOnlyKeysToDelete(idFieldName,
						idsToDelete));
				idsToDelete.clear();
			}
		}

		if (!idsToDelete.isEmpty()) {
			doBatchUpdateOrDelete(new TableWriteItems(getTable().getTableName())
					.withHashOnlyKeysToDelete(idFieldName, idsToDelete));
		}

	}

	protected PrimaryKey buildPrimaryKey(String idValue) {
		PrimaryKey primaryKey = new PrimaryKey();
		primaryKey.addComponent(getIdHandler().getIdFieldName(), idValue);

		return primaryKey;
	}

}
