package org.jstorni.lorm.mapping.strategies.entitytoitem;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;

import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.exceptions.DataValidationException;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.AttributeType;

public class DateEntityToItemMappingStrategy extends
		DefaultEntityToItemMappingStrategy {

	public DateEntityToItemMappingStrategy(ReflectionSupport reflectionSupport) {
		super(reflectionSupport);
	}

	@Override
	protected boolean checkAttribute(Field field, AttributeType attrType) {
		return attrType.equals(AttributeType.NUMBER);
	}

	@Override
	public boolean apply(Field field) {
		return Date.class.equals(field.getType());
	}

	@Override
	protected AttributeDefinition buildAttributeDefinition(Field field,
			String fieldNamePrefix) {
		// TODO get constraints
		return new AttributeDefinition(fieldNamePrefix + field.getName(),
				AttributeType.NUMBER, null);
	}

	@Override
	public void map(Object entity, Field field, String fieldNamePrefix,
			Map<AttributeDefinition, Object> attributes) {
		AttributeDefinition attrDef = buildAttributeDefinition(field,
				fieldNamePrefix);
		Object value = reflectionSupport.getValueOfField(field, entity);
		if (value == null) {
			attributes.put(attrDef, null);
			return;
		}

		// just check source, target should be checked at
		// startup only
		if (field.getType().equals(Date.class)) {
			attributes.put(attrDef, ((Date) value).getTime());
		} else {
			throw new DataValidationException("Incompatible field type ("
					+ field.getName() + "). " + "Expected: "
					+ Date.class.getName() + ". Actual: "
					+ value.getClass().getName());
		}

	}

}
