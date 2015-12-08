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
import org.jstorni.lorm.schema.validation.SchemaValidationErrorType;
import org.jstorni.lorm.schema.validation.SchemaValidationScope;
import org.jstorni.lorm.test.model.Expense;
import org.jstorni.lorm.test.model.Merchant;
import org.jstorni.lorm.test.model.embedded.SampleEmbeddable;
import org.jstorni.lorm.test.model.embedded.SampleEntity;
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

		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition("id",
				AttributeType.STRING, constraints));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"amount", AttributeType.NUMBER, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"category.id", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition("date",
				AttributeType.NUMBER, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"description", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"merchant.id", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"reporter.id", AttributeType.STRING, null));

		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"attachment.location", AttributeType.STRING, null));

		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"attachment.description", AttributeType.STRING, null));

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
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition("id",
				AttributeType.STRING, constraints));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"category.id", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"description", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"merchant.id", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"attachment.description", AttributeType.STRING, null));

		List<SchemaValidationError> negativeCaseValidationErrors = mapper
				.validateSchema(new EntitySchema("expense", attributes));
		Assert.assertFalse(negativeCaseValidationErrors.isEmpty());
		Assert.assertEquals(4, negativeCaseValidationErrors.size());

		for (SchemaValidationError error : negativeCaseValidationErrors) {
			String attrName = error.getAttributeDefinition().getName();
			Assert.assertTrue(attrName.equals("reporter.id")
					|| attrName.equals("amount") || attrName.equals("date")
					|| attrName.equals("attachment.location"));
		}

		List<AttributeDefinition> negativeMissingFieldsInEntityClass = mapper
				.getMissingAttributesInEntityClass(new EntitySchema("expense",
						attributes));
		Assert.assertTrue(negativeMissingFieldsInEntityClass.isEmpty());

		List<AttributeDefinition> negativeFieldsInTable = mapper
				.getMissingFieldsInTable(new EntitySchema("expense", attributes));
		Assert.assertFalse(negativeFieldsInTable.isEmpty());
		Assert.assertEquals(4, negativeFieldsInTable.size());

		for (AttributeDefinition attrDef : negativeFieldsInTable) {
			Assert.assertTrue(attrDef.getName().equals("reporter.id")
					|| attrDef.getName().equals("amount")
					|| attrDef.getName().equals("date")
					|| attrDef.getName().equals("attachment.location"));
		}

	}

	@Test
	public void testDeepEmbeddedMap() {
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.setSomeRandomField("random field");
		sampleEntity.setEmbedded(buildSampleEmbeddable(5));

		EntityToItemMapperImpl<SampleEntity> mapper = new EntityToItemMapperImpl<SampleEntity>(
				SampleEntity.class);

		Map<AttributeDefinition, Object> attrs = mapper.map(sampleEntity);

		String attrName = "embedded";
		for (int i = 5; i > 0; i--) {
			AttributeDefinition expectedAttr = new AttributeDefinition(attrName
					+ ".someField", AttributeType.STRING, null);
			Assert.assertTrue(attrs.containsKey(expectedAttr));
			Assert.assertEquals("some field " + i, attrs.get(expectedAttr));

			attrName = attrName + ".deepEmbedded";
		}
	}

	@Test
	public void testSchemaValidationWithRecursiveDependencies() {
		Set<AttributeConstraint> constraints = new HashSet<AttributeConstraint>();
		constraints.add(AttributeConstraint.buildPrimaryKeyConstraint());

		Set<org.jstorni.lorm.schema.AttributeDefinition> attributes = new HashSet<org.jstorni.lorm.schema.AttributeDefinition>();

		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition("id",
				AttributeType.STRING, constraints));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"someRandomField", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"embedded.someField", AttributeType.STRING, null));
		attributes.add(new org.jstorni.lorm.schema.AttributeDefinition(
				"embedded.deepEmbedded", AttributeType.STRING, null));

		EntityToItemMapperImpl<SampleEntity> mapper = new EntityToItemMapperImpl<SampleEntity>(
				SampleEntity.class);

		List<SchemaValidationError> positiveCaseValidationErrors = mapper
				.validateSchema(new EntitySchema("sampleentity", attributes));

		// TODO expect error of recursive dependency path
		Assert.assertEquals(1, positiveCaseValidationErrors.size());

		SchemaValidationError recursiveDependencyError = positiveCaseValidationErrors
				.get(0);
		Assert.assertNotNull(recursiveDependencyError);
		Assert.assertEquals(SchemaValidationErrorType.GENERAL,
				recursiveDependencyError.getErrorType());
		Assert.assertEquals("deepEmbedded",
				recursiveDependencyError.getIdentifier());
		Assert.assertEquals(SchemaValidationScope.ATTRIBUTE,
				recursiveDependencyError.getValidationScope());
		Assert.assertTrue(recursiveDependencyError.getMessage().contains(
				"recursive"));

	}

	private SampleEmbeddable buildSampleEmbeddable(int levels) {
		SampleEmbeddable sampleEmbeddable = new SampleEmbeddable();
		sampleEmbeddable.setSomeField("some field " + levels);
		if (levels > 0) {
			sampleEmbeddable.setDeepEmbedded(buildSampleEmbeddable(--levels));
		}

		return sampleEmbeddable;
	}

}
