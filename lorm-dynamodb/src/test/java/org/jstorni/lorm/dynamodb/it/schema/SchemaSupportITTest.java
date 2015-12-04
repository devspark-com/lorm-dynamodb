package org.jstorni.lorm.dynamodb.it.schema;

import java.util.ArrayList;
import java.util.List;

import org.jstorni.lorm.dynamodb.DynamoDBBaseRepository;
import org.jstorni.lorm.mapping.EntityToItemMapperImpl;
import org.jstorni.lorm.mapping.ItemToEntityMapper;
import org.jstorni.lorm.mapping.ItemToEntityMapperImpl;
import org.jstorni.lorm.schema.validation.SchemaValidationError;
import org.jstorni.lorm.test.model.Expense;
import org.jstorni.lorm.test.model.Merchant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class SchemaSupportITTest extends BaseIntegrationTest {

	@Before
	public void setup() {
		super.setup();
		
		addToEntityManager(Merchant.class);
		addToEntityManager(Expense.class);
	}

	@Test
	public void testValidateSchema() {
		EntityToItemMapperImpl<Expense> mapper = new EntityToItemMapperImpl<Expense>(
				Expense.class);

		List<AttributeDefinition> attrDefs = new ArrayList<AttributeDefinition>();
		attrDefs.add(new AttributeDefinition("id", ScalarAttributeType.S));

		List<KeySchemaElement> keys = new ArrayList<KeySchemaElement>();
		keys.add(new KeySchemaElement("id", KeyType.HASH));

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
		provisionedThroughput.setReadCapacityUnits(1L);
		provisionedThroughput.setWriteCapacityUnits(1L);

		// create util for table creation
		CreateTableRequest createTableReq = new CreateTableRequest()
				.withTableName("expense").withKeySchema(keys);
		createTableReq.setAttributeDefinitions(attrDefs);
		createTableReq.setProvisionedThroughput(provisionedThroughput);

		try {
			dynamoDB.deleteTable("expense");
		} catch (Exception ex) {

		}

		dynamoDB.createTable(createTableReq);

		// positive case
		ItemToEntityMapper<Expense> itemToEntityMapper = new ItemToEntityMapperImpl<Expense>(
				Expense.class, entityManager);

		DynamoDBBaseRepository<Expense> expenseRepository = new DynamoDBBaseRepository<Expense>(
				new DynamoDB(dynamoDB), mapper, itemToEntityMapper, mapper,
				Expense.class);

		List<SchemaValidationError> errors = new ArrayList<SchemaValidationError>();
		Assert.assertTrue(expenseRepository.isValid(errors));

	}

	@Test
	public void validateSchemaNegativeCase() {
		try {
			dynamoDB.deleteTable("merchant");
		} catch (Exception ex) {

		}

		// negative case
		EntityToItemMapperImpl<Merchant> merchantMapper = new EntityToItemMapperImpl<Merchant>(
				Merchant.class);
		ItemToEntityMapper<Merchant> merchantItemToEntityMapper = new ItemToEntityMapperImpl<Merchant>(
				Merchant.class, entityManager);

		DynamoDBBaseRepository<Merchant> merchantRepository = new DynamoDBBaseRepository<Merchant>(
				new DynamoDB(dynamoDB), merchantMapper,
				merchantItemToEntityMapper, merchantMapper, Merchant.class);
		List<SchemaValidationError> merchantSchemaValidationErrors = new ArrayList<SchemaValidationError>();
		Assert.assertFalse(merchantRepository
				.isValid(merchantSchemaValidationErrors));
		Assert.assertTrue(merchantRepository.syncToSchema(false, true, false));
		merchantSchemaValidationErrors.clear();
		Assert.assertTrue(merchantRepository
				.isValid(merchantSchemaValidationErrors));

	}

}
