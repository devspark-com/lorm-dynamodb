package org.devspark.aws.lorm.dynamodb.it.schema;

import java.util.ArrayList;

import org.devspark.aws.lorm.EntityManager;
import org.devspark.aws.lorm.SchemaSupport;
import org.devspark.aws.lorm.dynamodb.DynamoDBEntityManager;
import org.devspark.aws.lorm.mapping.EntityToItemMapper;
import org.devspark.aws.lorm.mapping.EntityToItemMapperImpl;
import org.devspark.aws.lorm.mapping.ItemToEntityMapper;
import org.devspark.aws.lorm.mapping.ItemToEntityMapperImpl;
import org.devspark.aws.lorm.schema.validation.EntitySchemaSupport;
import org.devspark.aws.lorm.schema.validation.SchemaValidationError;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class BaseIntegrationTest {
	private static final String PORT = System.getProperty("dynamodb.port");
	private static final String HOST = System.getProperty("dynamodb.host");

	protected EntityManager entityManager;
	protected AmazonDynamoDB dynamoDB;

	protected void setup() {
		if (System.getProperty("dynamodb.host") == null) {
			setupRemote();
		} else {
			setupLocal();
		}
	}

	protected void tearDown() {
		dynamoDB.shutdown();
	}
	
	public void setupLocal() {
		String host = HOST != null ? HOST : "localhost";
		String port = PORT != null ? PORT : "8000";

		entityManager = new DynamoDBEntityManager(host, port, "", "");
		dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
		dynamoDB.setEndpoint("http://" + host + ":" + port);
	}

	public void setupRemote() {
		entityManager = new DynamoDBEntityManager(HOST, PORT, null, null);
		dynamoDB = new AmazonDynamoDBClient();
		if (HOST != null) {
			String url = "http://" + HOST;
			if (PORT != null) {
				url = url + ":" + PORT;
			}
			dynamoDB.setEndpoint("http://" + HOST + ":" + PORT);
		}
	}

	protected <T> void addToEntityManager(Class<T> entityClass) {
		EntityToItemMapper entityToItemMapper = new EntityToItemMapperImpl<T>(
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
