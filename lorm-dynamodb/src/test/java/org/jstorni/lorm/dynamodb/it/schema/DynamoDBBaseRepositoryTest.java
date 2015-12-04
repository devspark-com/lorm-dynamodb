package org.jstorni.lorm.dynamodb.it.schema;

import java.util.ArrayList;
import java.util.List;

import org.jstorni.lorm.Repository;
import org.jstorni.lorm.test.model.Expense;
import org.jstorni.lorm.test.model.Merchant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DynamoDBBaseRepositoryTest extends BaseIntegrationTest {

	@Before
	public void setup() {
		super.setup();

		addToEntityManager(Merchant.class);
		addToEntityManager(Expense.class);

		setupSchemaForEntity(Merchant.class);
		setupSchemaForEntity(Expense.class);

		// remove all merchants
		Repository<Merchant> merchantRepository = entityManager
				.getRepository(Merchant.class);

		List<String> existingMerchantIds = new ArrayList<String>();
		List<Merchant> existingMerchants = merchantRepository.findAll();
		for (Merchant merchant : existingMerchants) {
			existingMerchantIds.add(merchant.getId());
		}
		merchantRepository.deleteById(existingMerchantIds);

		// remove all expenses
		Repository<Expense> expenseRepository = entityManager
				.getRepository(Expense.class);

		List<String> existingExpenseIds = new ArrayList<String>();
		List<Expense> existingExpenses = expenseRepository.findAll();
		for (Expense expense : existingExpenses) {
			existingExpenseIds.add(expense.getId());
		}
		expenseRepository.deleteById(existingExpenseIds);

	}

	private Merchant buildMerchant(String name) {
		Merchant merchant = new Merchant();
		merchant.setName(name);

		return merchant;
	}

	@Test
	public void testSingle() {
		Repository<Merchant> repository = entityManager
				.getRepository(Merchant.class);

		// create one
		Merchant merchant = buildMerchant("new merchant");

		Merchant savedMerchant = repository.save(merchant);

		Assert.assertTrue(merchant == savedMerchant); // same reference
		Assert.assertNotNull(merchant.getId());
		Assert.assertEquals("new merchant", merchant.getName());

		// find by id
		Merchant foundMerchant = repository.findOne(merchant.getId());
		Assert.assertNotNull(foundMerchant);
		Assert.assertEquals(merchant.getId(), foundMerchant.getId());
		Assert.assertEquals("new merchant", foundMerchant.getName());
		
		// update
		foundMerchant.setName("updated name");
		repository.save(foundMerchant);
		Merchant updatedMerchant = repository.findOne(merchant.getId());
		Assert.assertNotNull(updatedMerchant);
		Assert.assertEquals(merchant.getId(), updatedMerchant.getId());
		Assert.assertEquals("updated name", updatedMerchant.getName());
		
		// delete
		repository.deleteById(merchant.getId());
		Merchant deletedMerchant = repository.findOne(merchant.getId());
		Assert.assertNull(deletedMerchant);
	}

	@Test
	public void testBatch() {
		Repository<Merchant> repository = entityManager
				.getRepository(Merchant.class);

		// add 100
		List<Merchant> merchants = new ArrayList<Merchant>();
		for (int i = 0; i < 100; i++) {
			Merchant merchant = buildMerchant("sample merchant #" + i);
			merchants.add(merchant);
		}

		repository.save(merchants);

		// fetch recently added
		List<Merchant> savedMerchants = repository.findAll();

		Assert.assertNotNull(savedMerchants);
		Assert.assertEquals(merchants.size(), savedMerchants.size());

		for (Merchant merchant : merchants) {
			boolean found = false;
			for (Merchant savedMerchant : savedMerchants) {
				if (merchant.getId().equals(savedMerchant.getId())) {
					Assert.assertEquals(merchant.getName(), savedMerchant.getName());
					found = true;
					break;
				}
			}

			Assert.assertTrue("Merchant not found", found);
		}
		
		// update all
		for (Merchant merchant : merchants) {
			merchant.setName("updated name");
		}
		
		repository.save(merchants);

		// check batch update
		List<Merchant> updatedMerchants = repository.findAll();

		Assert.assertNotNull(updatedMerchants);
		Assert.assertEquals(merchants.size(), updatedMerchants.size());

		for (Merchant merchant : merchants) {
			boolean found = false;
			for (Merchant updatedMerchant : updatedMerchants) {
				if (merchant.getId().equals(updatedMerchant.getId())) {
					Assert.assertEquals("updated name", updatedMerchant.getName());
					found = true;
					break;
				}
			}

			Assert.assertTrue("Merchant not found", found);
		}
		
		
		
	}

}
