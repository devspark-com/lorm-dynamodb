package org.jstorni.lorm.id;

import org.jstorni.lorm.id.EntityIdHandler;
import org.jstorni.lorm.test.model.Merchant;
import org.junit.Assert;
import org.junit.Test;

public class EntityIdHandlerTest {

	@Test
	public void testGetIdValue() {
		Merchant merchant = new Merchant();
		merchant.setName("something");
		merchant.setId("123456");

		EntityIdHandler entityIdHandler = new EntityIdHandler(
				Merchant.class);

		Assert.assertEquals("Id", "123456",
				entityIdHandler.getIdValue(merchant));
	}

	@Test
	public void testSetIdValue() {
		Merchant merchant = new Merchant();
		merchant.setName("something");

		EntityIdHandler entityIdHandler = new EntityIdHandler(
				Merchant.class);
		entityIdHandler.setIdValue(merchant, "123456");

		Assert.assertEquals("Id", "123456",
				entityIdHandler.getIdValue(merchant));
	}

	@Test
	public void testGenerateId() {
		Merchant merchant = new Merchant();
		merchant.setName("something");

		EntityIdHandler entityIdHandler = new EntityIdHandler(
				Merchant.class);
		entityIdHandler.setIdValue(merchant,
				entityIdHandler.generateId(merchant));

		Assert.assertNotNull("Id", merchant.getId());

	}

}
