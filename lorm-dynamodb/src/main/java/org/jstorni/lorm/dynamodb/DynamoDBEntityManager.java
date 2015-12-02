package org.jstorni.lorm.dynamodb;

import java.util.HashMap;
import java.util.Map;

import org.jstorni.lorm.AbstractEntityManagerImpl;
import org.jstorni.lorm.mapping.EntityToItemMapper;
import org.jstorni.lorm.mapping.ItemToEntityMapper;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class DynamoDBEntityManager extends AbstractEntityManagerImpl {
	private AmazonDynamoDB dynamoDB;

	public final static String HOST = "HOST";
	public final static String PORT = "PORT";
	public final static String USERNAME = "USERNAME";
	public final static String PASSWORD = "PASSWORD";

	public DynamoDBEntityManager(String host, int port, String username,
			String password) {
		Map<String, String> properties = new HashMap<String, String>();
		properties.put(HOST, host);
		properties.put(PORT, String.valueOf(port));
		properties.put(USERNAME, username);
		properties.put(PASSWORD, password);
		
		setUp(properties);
	}

	@Override
	protected void setUp(Map<String, String> properties) {
		if (dynamoDB != null) {
			return;
		}

		dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials(
				properties.get(USERNAME), properties.get(PASSWORD)));
		dynamoDB.setEndpoint("http://" + properties.get(HOST) + ":"
				+ properties.get(PORT));
	}

	@Override
	public <T> void addEntity(Class<T> entityClass,
			EntityToItemMapper<T> entityToItemMapper,
			ItemToEntityMapper<T> itemToEntityMapper,
			EntitySchemaSupport entitySchemaSupport) {
		getAllRepositories().put(
				entityClass,
				new DynamoDBBaseRepository<T>(new DynamoDB(dynamoDB),
						entityToItemMapper, itemToEntityMapper,
						entitySchemaSupport, entityClass));
	}

}
