package org.jstorni.lorm.mapping.strategies.itemtoentity;

import java.lang.reflect.Field;
import java.util.Map.Entry;

import javax.persistence.ManyToOne;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jstorni.lorm.EntityManager;
import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.Repository;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.schema.AttributeDefinition;

public class ManyToOneItemToEntityMappingStrategy implements
		ItemToEntityMappingStrategy {

	private final ReflectionSupport reflectionSupport;
	private final EntityManager entityManager;
	private final Log log = LogFactory.getLog(getClass());

	public ManyToOneItemToEntityMappingStrategy(
			ReflectionSupport reflectionSupport, EntityManager entityManager) {
		super();
		this.reflectionSupport = reflectionSupport;
		this.entityManager = entityManager;
	}

	@Override
	public boolean apply(Field field) {
		return field.getAnnotation(ManyToOne.class) != null;
	}

	@Override
	public void map(Entry<AttributeDefinition, Object> itemEntry, Field field,
			Object entityInstance) {
		if (itemEntry.getValue() == null) {
			return;
		}

		String fieldKey = itemEntry.getKey().getName();

		if (!itemEntry.getValue().getClass().equals(String.class)) {
			throw new DataValidationException("Invalid ID type ("
					+ itemEntry.getValue().getClass().getName()
					+ ") for entity (" + entityInstance.getClass().getName()
					+ ") - Item component: " + fieldKey);
		}

		if (fieldKey.contains(".")) {
			fieldKey = fieldKey.substring(0, fieldKey.indexOf('.'));
		} else {
			throw new DataValidationException(
					"Invalid ManyToOne attribute name: " + fieldKey
							+ " for entity class: "
							+ entityInstance.getClass().getName());
		}

		String refId = (String) itemEntry.getValue();
		Repository<?> refRepository = entityManager.getRepository(field
				.getType());
		Object refInstance = refRepository.findOne(refId);
		if (refInstance != null) {
			reflectionSupport.setValueOfField(field, entityInstance,
					refInstance);
		} else {
			log.warn("Reference not found " + field.getType() + " with id: "
					+ refId + " for entity "
					+ entityInstance.getClass().getName());
		}

	}

}
