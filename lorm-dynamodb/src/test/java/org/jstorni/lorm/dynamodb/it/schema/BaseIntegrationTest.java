package org.jstorni.lorm.dynamodb.it.schema;

import java.util.ArrayList;

import org.jstorni.lorm.EntityManager;
import org.jstorni.lorm.SchemaSupport;
import org.jstorni.lorm.dynamodb.DynamoDBEntityManager;
import org.jstorni.lorm.mapping.EntityToItemMapper;
import org.jstorni.lorm.mapping.EntityToItemMapperImpl;
import org.jstorni.lorm.mapping.ItemToEntityMapper;
import org.jstorni.lorm.mapping.ItemToEntityMapperImpl;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;
import org.jstorni.lorm.schema.validation.SchemaValidationError;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class BaseIntegrationTest {
	private static final String PORT = System.getProperty("dynamodb.port",
			"8000");
	private static final String HOST = System.getProperty("dynamodb.host",
			"localhost");

	protected EntityManager entityManager;
	protected AmazonDynamoDB dynamoDB;

	public void setUp() {
		entityManager = new DynamoDBEntityManager(HOST, Integer.valueOf(PORT),
				"", "");
		dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
		dynamoDB.setEndpoint("http://" + HOST + ":" + PORT);

	}

	protected <T> void addToEntityManager(Class<T> entityClass) {
		EntityToItemMapper<T> entityToItemMapper = new EntityToItemMapperImpl<T>(
				entityClass);
		EntitySchemaSupport entitySchemaSupport = new EntityToItemMapperImpl<T>(
				entityClass);
		ItemToEntityMapper<T> itemToEntityMapper = new ItemToEntityMapperImpl<T>(
				entityClass, entityManager);

		entityManager.addEntity(entityClass, entityToItemMapper,
				itemToEntityMapper, entitySchemaSupport);
	}

	@SuppressWarnings("unchecked")
	protected <T> void setupSchemaForEntity(Class<T> entityClass) {
		SchemaSupport<T> repository = (SchemaSupport<T>) entityManager
				.getRepository(entityClass);
		if (!repository.isValid(new ArrayList<SchemaValidationError>())) {
			repository.syncToSchema(true, true, true);
		}
	}
}
