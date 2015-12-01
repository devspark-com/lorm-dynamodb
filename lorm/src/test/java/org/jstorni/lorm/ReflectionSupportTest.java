package org.jstorni.lorm;

import java.lang.reflect.Field;

import javax.persistence.Id;

import org.jstorni.lorm.ReflectionSupport;
import org.junit.Assert;
import org.junit.Test;

public class ReflectionSupportTest {

	@Test
	public void testGetFieldWithAnnotation() {
		ReflectionSupport refSupport = new ReflectionSupport();

		DerivedDummy paramClass = new DerivedDummy();
		Field idField = refSupport.getFieldWithAnnotation(
				paramClass.getClass(), Id.class);

		Assert.assertEquals("id", idField.getName());
	}

	@Test
	public void testGetValueOfField() {
		DerivedDummy instance = new DerivedDummy();
		instance.setId("123456");

		ReflectionSupport refSupport = new ReflectionSupport();
		Field idField = refSupport.getFieldWithAnnotation(instance.getClass(),
				Id.class);

		Assert.assertEquals("123456",
				refSupport.getValueOfField(idField, instance));

	}

	@Test
	public void testSetValueOfField() {
		DerivedDummy instance = new DerivedDummy();

		ReflectionSupport refSupport = new ReflectionSupport();
		Field idField = refSupport.getFieldWithAnnotation(instance.getClass(),
				Id.class);

		refSupport.setValueOfField(idField, instance, "123456");
		
		Assert.assertEquals("123456",
				refSupport.getValueOfField(idField, instance));

	}

}

class DerivedDummy extends DummyClass<String> {
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}

class DummyClass<T> {

	@Id
	private String id;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
