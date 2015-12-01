package org.jstorni.lorm.mapper.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jstorni.lorm.DummyEntityManager;
import org.jstorni.lorm.mapping.ItemToEntityMapperImpl;
import org.jstorni.lorm.schema.AttributeConstraint;
import org.jstorni.lorm.schema.AttributeDefinition;
import org.jstorni.lorm.schema.AttributeType;
import org.jstorni.lorm.test.model.Category;
import org.jstorni.lorm.test.model.Expense;
import org.jstorni.lorm.test.model.Merchant;
import org.jstorni.lorm.test.model.Reporter;
import org.junit.Assert;
import org.junit.Test;

public class ItemToEntityMapperImplTest {

	@Test
	public void testMap() {
		Category category = new Category();
		category.setId("categoryid");
		category.setDescription("category description");
		DummyEntityManager.get().getRepository(Category.class).save(category);

		Merchant merchant = new Merchant();
		merchant.setId("merchantid");
		merchant.setName("merchant name");
		DummyEntityManager.get().getRepository(Merchant.class).save(merchant);

		Reporter reporter = new Reporter();
		reporter.setId("reporterid");
		reporter.setName("reporter name");
		DummyEntityManager.get().getRepository(Reporter.class).save(reporter);

		Map<AttributeDefinition, Object> attributes = new HashMap<AttributeDefinition, Object>();
		Set<AttributeConstraint> pkConstraints = new HashSet<AttributeConstraint>();
		pkConstraints.add(AttributeConstraint.buildPrimaryKeyConstraint());
		attributes.put(new AttributeDefinition("id", AttributeType.STRING, pkConstraints), "expenseid");
		attributes.put(new AttributeDefinition("description", AttributeType.STRING, null), "expense description");
		attributes.put(new AttributeDefinition("category.id", AttributeType.STRING, null), "categoryid");
		attributes.put(new AttributeDefinition("merchant.id", AttributeType.STRING, null), "merchantid");
		attributes.put(new AttributeDefinition("reporter.id", AttributeType.STRING, null), "reporterid");
		attributes.put(new AttributeDefinition("date", AttributeType.NUMBER, null), 999999);

		ItemToEntityMapperImpl<Expense> expenseMapper = new ItemToEntityMapperImpl<Expense>(
				Expense.class, DummyEntityManager.get());
		Expense expense = expenseMapper.map(attributes);

		Assert.assertEquals("expenseid", expense.getId());
		Assert.assertEquals("merchantid", expense.getMerchant().getId());
		Assert.assertEquals("categoryid", expense.getCategory().getId());
		Assert.assertEquals("reporterid", expense.getReporter().getId());
		Assert.assertEquals(999999, expense.getDate().getTime());
	}

}
