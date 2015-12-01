package org.jstorni.lorm.mapping;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.jstorni.lorm.EntityManager;
import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.id.EntityIdHandler;
import org.jstorni.lorm.mapping.strategies.itemtoentity.DateItemToEntityMappingStrategy;
import org.jstorni.lorm.mapping.strategies.itemtoentity.DefaultItemToEntityMappingStrategy;
import org.jstorni.lorm.mapping.strategies.itemtoentity.ItemToEntityMappingStrategy;
import org.jstorni.lorm.mapping.strategies.itemtoentity.ManyToOneItemToEntityMappingStrategy;
import org.jstorni.lorm.mapping.strategies.itemtoentity.OneToManyItemToEntityMappingStrategy;
import org.jstorni.lorm.mapping.strategies.itemtoentity.TransientItemToEntityMappingStrategy;
import org.jstorni.lorm.schema.AttributeDefinition;

public class ItemToEntityMapperImpl<E> implements ItemToEntityMapper<E> {
	private final Class<E> entityClass;
	private final List<ItemToEntityMappingStrategy> mappingStrategies;

	public ItemToEntityMapperImpl(Class<E> entityClass,
			EntityManager entityManager) {
		super();
		this.entityClass = entityClass;

		ReflectionSupport reflectionSupport = new ReflectionSupport();

		mappingStrategies = new ArrayList<ItemToEntityMappingStrategy>();
		mappingStrategies.add(new TransientItemToEntityMappingStrategy());
		mappingStrategies.add(new OneToManyItemToEntityMappingStrategy());
		mappingStrategies.add(new ManyToOneItemToEntityMappingStrategy(
				reflectionSupport, entityManager));
		mappingStrategies.add(new DateItemToEntityMappingStrategy(
				reflectionSupport));
		mappingStrategies.add(new DefaultItemToEntityMappingStrategy(
				reflectionSupport));
	}

	@Override
	public E map(Map<AttributeDefinition, Object> attributes) {
		ReflectionSupport reflectionSupport = new ReflectionSupport();

		List<Field> fields = reflectionSupport.getAllFields(entityClass);
		E instance = reflectionSupport.instantiate(entityClass);
		for (Entry<AttributeDefinition, Object> itemEntry : attributes.entrySet()) {
			String fieldKey = itemEntry.getKey().getName();
			if (fieldKey.contains(".")) {
				fieldKey = fieldKey.substring(0, fieldKey.indexOf('.'));
			}

			Field field = getFieldByName(fieldKey, fields);
			if (field == null) {
				throw new DataValidationException(
						"Field not found. Attribute name: " + fieldKey
								+ " .Entity class: " + entityClass.getName());
			}

			for (ItemToEntityMappingStrategy mappingStrategy : mappingStrategies) {
				if (mappingStrategy.apply(field)) {
					mappingStrategy.map(itemEntry, field, instance);
					break;
				}
			}
		}

		// validate primary key mapping
		EntityIdHandler<E> idHandler = new EntityIdHandler<E>(entityClass);
		String id = idHandler.getIdValue(instance);
		if (id == null) {
			throw new DataValidationException(
					"Primary key not found in item when "
							+ "mapping to entity " + entityClass.getName());
		}

		return instance;
	}

	private Field getFieldByName(String fieldName, List<Field> fields) {
		Field field = null;

		for (Field currentField : fields) {
			if (currentField.getName().equals(fieldName)) {
				field = currentField;
				break;
			}
		}

		return field;
	}

}
