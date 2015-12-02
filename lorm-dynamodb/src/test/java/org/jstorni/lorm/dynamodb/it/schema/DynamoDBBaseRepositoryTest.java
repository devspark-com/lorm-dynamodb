package org.jstorni.lorm.dynamodb.it.schema;

import static org.junit.Assert.fail;

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
	public void setUp() {
		super.setUp();

		addToEntityManager(Merchant.class);
		addToEntityManager(Expense.class);

		setupSchemaForEntity(Merchant.class);
		setupSchemaForEntity(Expense.class);
	}

	private Merchant buildMerchant(String name) {
		Merchant merchant = new Merchant();
		merchant.setName(name);

		return merchant;
	}

	@Test
	public void testFindOne() {
		Repository<Merchant> repository = entityManager
				.getRepository(Merchant.class);

		Merchant merchant = buildMerchant("new merchant");

		Merchant savedMerchant = repository.save(merchant);

		Assert.assertTrue(merchant == savedMerchant); // same reference
		Assert.assertNotNull(merchant.getId());
		Assert.assertEquals("new merchant", merchant.getName());

		Merchant foundMerchant = repository.findOne(merchant.getId());
		Assert.assertNotNull(foundMerchant);
		Assert.assertEquals(merchant.getId(), foundMerchant.getId());
		Assert.assertEquals("new merchant", foundMerchant.getName());
	}

	@Test
	public void testFindAll() {
		Repository<Merchant> repository = entityManager
				.getRepository(Merchant.class);

		// remove all
		List<String> existingMerchantIds = new ArrayList<String>();
		List<Merchant> existingMerchants = repository.findAll();
		for (Merchant merchant : existingMerchants) {
			existingMerchantIds.add(merchant.getId());
		}
		repository.deleteById(existingMerchantIds);
		
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
		Assert.assertTrue("Expected 100 merchants",
				savedMerchants.size() == 100);

		for (Merchant merchant : merchants) {
			boolean found = false;
			for (Merchant savedMerchant : savedMerchants) {
				if (merchant.getId().equals(savedMerchant.getId())) {
					found = true;
					break;
				}
			}

			Assert.assertTrue("Merchant not found", found);
		}

	}

	@Test
	public void testSaveT() {
		fail("Not yet implemented");
	}

	@Test
	public void testSaveListOfT() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteByIdString() {
		fail("Not yet implemented");
	}

	@Test
	public void testDeleteByIdListOfString() {
		fail("Not yet implemented");
	}

}
