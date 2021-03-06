package org.devspark.aws.lorm.dynamodb.it.schema;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.devspark.aws.lorm.dynamodb.DynamoDBBaseRepository;
import org.devspark.aws.lorm.mapping.EntityToItemMapperImpl;
import org.devspark.aws.lorm.mapping.ItemToEntityMapper;
import org.devspark.aws.lorm.mapping.ItemToEntityMapperImpl;
import org.devspark.aws.lorm.schema.validation.SchemaValidationError;
import org.devspark.aws.lorm.test.model.Expense;
import org.devspark.aws.lorm.test.model.Merchant;
import org.junit.After;
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
    private final static Log LOG = LogFactory.getLog(SchemaSupportITTest.class);

    @Before
    public void setup() {
        super.setup();

        addToEntityManager(Merchant.class);
        addToEntityManager(Expense.class);
    }

    @After
    public void tearDown() {
        super.tearDown();
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

        deleteTable("expense");

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
        deleteTable("merchant");

        // negative case
        EntityToItemMapperImpl<Merchant> merchantMapper = new EntityToItemMapperImpl<Merchant>(
                Merchant.class);
        ItemToEntityMapper<Merchant> merchantItemToEntityMapper = new ItemToEntityMapperImpl<Merchant>(
                Merchant.class, entityManager);

        DynamoDBBaseRepository<Merchant> merchantRepository = new DynamoDBBaseRepository<Merchant>(
                new DynamoDB(dynamoDB), merchantMapper, merchantItemToEntityMapper,
                merchantMapper, Merchant.class);
        List<SchemaValidationError> merchantSchemaValidationErrors = new ArrayList<SchemaValidationError>();
        Assert.assertFalse(merchantRepository.isValid(merchantSchemaValidationErrors));
        Assert.assertTrue(merchantRepository.syncToSchema(false, true, false));
        merchantSchemaValidationErrors.clear();
        Assert.assertTrue(merchantRepository.isValid(merchantSchemaValidationErrors));

    }

}
