package org.jstorni.lorm;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jstorni.lorm.exceptions.DataException;

public class ReflectionSupport {

	public Field getFieldWithAnnotation(Class<?> sourceClass,
			Class<? extends Annotation> annotationClass) {
		Field field = null;
		List<Field> fields = getAllFields(sourceClass);
		for (Field currentField : fields) {
			Annotation annotation = currentField.getAnnotation(annotationClass);
			if (annotation != null) {
				field = currentField;
				break;
			}
		}

		if (field == null) {
			throw new DataException("Id attribute not found in class "
					+ sourceClass.getName());
		}

		return field;
	}

	public Object getValueOfField(Field field, Object instance) {
		Object value;
		try {
			if (!field.isAccessible()) {
				field.setAccessible(true);
			}

			value = field.get(instance);
		} catch (Exception ex) {
			throw new DataException("Error while obtaining value from field ["
					+ field.getName() + "] and class ["
					+ instance.getClass().getName() + "]", ex);
		}

		return value;
	}

	public void setValueOfField(Field field, Object instance, Object value) {
		try {
			if (!field.isAccessible()) {
				field.setAccessible(true);
			}

			field.set(instance, value);
		} catch (Exception ex) {
			throw new DataException("Error while assigning value [" + value
					+ "] to field [" + field.getName() + "] and class ["
					+ instance.getClass().getName() + "]", ex);
		}
	}

	public List<Field> getAllFields(Class<?> sourceClass) {
		List<Field> fields = new ArrayList<Field>();

		Class<?> currentClass = sourceClass;
		while (currentClass != null && !currentClass.equals(Object.class)) {
			fields.addAll(Arrays.asList(currentClass.getDeclaredFields()));
			currentClass = currentClass.getSuperclass();
		}

		return fields;
	}

	public Set<String> getAllFieldNames(Class<?> sourceClass) {
		Set<String> fieldNames = new HashSet<String>();
		
		List<Field> fields = getAllFields(sourceClass);
		for (Field field : fields) {
			fieldNames.add(field.getName());
		}
		
		return fieldNames;
	}
	
	public <T> T instantiate(Class<T> clazz) {
		T instance;

		try {
			instance = clazz.newInstance();
		} catch (Exception ex) {
			throw new DataException("Cannot instantiate: " + clazz.getName(),
					ex);
		}

		return instance;
	}

}
