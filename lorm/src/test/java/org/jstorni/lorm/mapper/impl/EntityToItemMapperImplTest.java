package org.jstorni.lorm.mapper.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jstorni.lorm.mapping.EntityToItemMapperImpl;
import org.jstorni.lorm.schema.AttributeConstraint;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.AttributeType;
import org.jstorni.lorm.schema.EntitySchema;
import org.jstorni.lorm.schema.validation.SchemaValidationError;
import org.jstorni.lorm.test.model.Expense;
import org.jstorni.lorm.test.model.Merchant;
import org.junit.Assert;
import org.junit.Test;

public class EntityToItemMapperImplTest {

	private AttributeDefinition buildAttrDefByName(String name) {
		return new AttributeDefinition(name, null, null);
	}

	@Test
	public void testSimpleMap() {
		EntityToItemMapperImpl<Merchant> mapper = new EntityToItemMapperImpl<Merchant>(
				Merchant.class);

		Merchant merchant = new Merchant();
		merchant.setId("123456");
		merchant.setName("something");

		Map<AttributeDefinition, Object> attrs = mapper.map(merchant);

		Assert.assertEquals("123456", attrs.get(buildAttrDefByName("id")));
		Assert.assertEquals("something", attrs.get(buildAttrDefByName("name")));
	}

	@Test
	public void testManyToOneMap() {
		EntityToItemMapperImpl<Expense> mapper = new EntityToItemMapperImpl<Expense>(
				Expense.class);

		Merchant merchant = new Merchant();
		merchant.setId("merchantid");
		merchant.setName("merchantdescription");

		Expense expense = new Expense();
		expense.setId("expenseid");
		expense.setDescription("expensedescription");
		expense.setMerchant(merchant);

		Map<AttributeDefinition, Object> attrs = mapper.map(expense);
		Assert.assertEquals("expenseid", attrs.get(buildAttrDefByName("id")));
		Assert.assertEquals("expensedescription",
				attrs.get(buildAttrDefByName("description")));
		Assert.assertEquals("merchantid",
				attrs.get(buildAttrDefByName("merchant.id")));

	}

	@Test
	public void validateSchema() {
		Set<AttributeConstraint> constraints = new HashSet<AttributeConstraint>();
		constraints.add(AttributeConstraint.buildPrimaryKeyConstraint());

		Set<org.jstorni.lorm.schema.AttributeDefinition> attributes = new HashSet<org.jstorni.lorm.schema.AttributeDefinition>();

		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"id", AttributeType.STRING, constraints));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"amount", AttributeType.NUMBER, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"category.id", AttributeType.STRING, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"date", AttributeType.NUMBER, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"description", AttributeType.STRING, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"merchant.id", AttributeType.STRING, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"reporter.id", AttributeType.STRING, null));

		EntityToItemMapperImpl<Expense> mapper = new EntityToItemMapperImpl<Expense>(
				Expense.class);

		List<SchemaValidationError> positiveCaseValidationErrors = mapper
				.validateSchema(new EntitySchema("expense", attributes));
		Assert.assertTrue(positiveCaseValidationErrors.isEmpty());

		List<AttributeDefinition> missingFieldsInEntityClass = mapper
				.getMissingAttributesInEntityClass(new EntitySchema("expense",
						attributes));
		Assert.assertTrue(missingFieldsInEntityClass.isEmpty());

		List<AttributeDefinition> missingFieldsInTable = mapper
				.getMissingAttributesInEntityClass(new EntitySchema("expense",
						attributes));
		Assert.assertTrue(missingFieldsInTable.isEmpty());

		attributes.clear();
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"id", AttributeType.STRING, constraints));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"category.id", AttributeType.STRING, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"description", AttributeType.STRING, null));
		attributes
				.add(new org.jstorni.lorm.schema.AttributeDefinition(
						"merchant.id", AttributeType.STRING, null));

		List<SchemaValidationError> negativeCaseValidationErrors = mapper
				.validateSchema(new EntitySchema("expense", attributes));
		Assert.assertFalse(negativeCaseValidationErrors.isEmpty());
		Assert.assertEquals(negativeCaseValidationErrors.size(), 3);

		for (SchemaValidationError error : negativeCaseValidationErrors) {
			String attrName = error.getAttributeDefinition().getName();
			Assert.assertTrue(attrName.equals("reporter.id")
					|| attrName.equals("amount") || attrName.equals("date"));
		}

		List<AttributeDefinition> negativeMissingFieldsInEntityClass = mapper
				.getMissingAttributesInEntityClass(new EntitySchema("expense",
						attributes));
		Assert.assertTrue(negativeMissingFieldsInEntityClass.isEmpty());

		List<AttributeDefinition> negativeFieldsInTable = mapper
				.getMissingFieldsInTable(new EntitySchema("expense", attributes));
		Assert.assertFalse(negativeFieldsInTable.isEmpty());
		Assert.assertEquals(negativeFieldsInTable.size(), 3);

		for (AttributeDefinition attrDef : negativeFieldsInTable) {
			Assert.assertTrue(attrDef.getName().equals("reporter.id")
					|| attrDef.getName().equals("amount")
					|| attrDef.getName().equals("date"));
		}

	}
}
