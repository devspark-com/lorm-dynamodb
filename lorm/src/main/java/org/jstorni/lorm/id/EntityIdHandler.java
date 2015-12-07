package org.jstorni.lorm.id;

import java.lang.reflect.Field;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.jstorni.lorm.ReflectionSupport;
import org.jstorni.lorm.exceptions.DataException;

public class EntityIdHandler {

	private final ReflectionSupport reflectionSupport;
	private final Field idField;

	public EntityIdHandler(Class<?> entityClass) {
		this.reflectionSupport = new ReflectionSupport();
		idField = reflectionSupport.getFieldWithAnnotation(entityClass,
				Id.class);
	}

	public String getIdFieldName() {
		// TODO assuming String id
		return idField.getName();
	}

	public String getIdValue(Object entity) {
		Object idValue = reflectionSupport.getValueOfField(idField, entity);
		// TODO assuming String id
		return (String) idValue;
	}

	public void setIdValue(Object entity, String id) {
		// TODO assuming String id
		reflectionSupport.setValueOfField(idField, entity, id);
	}

	public String generateId(Object entity) {
		GeneratedValue idAnnotation = idField
				.getAnnotation(GeneratedValue.class);
		if (idAnnotation == null || idAnnotation.generator() == null) {
			return null;
		}

		// TODO assuming String id
		String id = null;
		try {
			Class<?> idGeneratorClass = Class.forName(idAnnotation.generator());
			if (!IdGenerator.class.isAssignableFrom(idGeneratorClass)) {
				throw new DataException("Invalid id generator class: "
						+ idAnnotation.generator());
			}

			IdGenerator idGenerator = (IdGenerator) idGeneratorClass
					.newInstance();
			id = idGenerator.generateId();

		} catch (ClassNotFoundException e) {
			throw new DataException("Id generator class not found: "
					+ idAnnotation.generator());
		} catch (IllegalAccessException ex) {
			throw new DataException("Cannot instantiate id generator: "
					+ idAnnotation.generator());
		} catch (InstantiationException ex) {
			throw new DataException("Cannot instantiate id generator: "
					+ idAnnotation.generator());
		}

		return id;
	}
}
