package org.jstorni.lorm.mapping.strategies.itemtoentity;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map.Entry;

import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.schema.AttributeDefinition;

public class DateItemToEntityMappingStrategy implements
		ItemToEntityMappingStrategy {

	private final ReflectionSupport reflectionSupport;

	public DateItemToEntityMappingStrategy(ReflectionSupport reflectionSupport) {
		super();
		this.reflectionSupport = reflectionSupport;
	}

	@Override
	public boolean apply(Field field) {
		return Date.class.equals(field.getType());
	}

	@Override
	public void map(Entry<AttributeDefinition, Object> itemEntry, Field field,
			Object entityInstance) {
		Object value = itemEntry.getValue();
		if (value == null) {
			return;
		}

		if (Number.class.isAssignableFrom(value.getClass())) {
			reflectionSupport.setValueOfField(field, entityInstance, new Date(
					((Number) value).longValue()));

		} else {
			throw new DataValidationException("Incompatible item attribute ("
					+ field.getName() + "). " + "Expected: "
					+ Number.class.getName() + ". Actual: "
					+ value.getClass().getName());
		}

	}

}
