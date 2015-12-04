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
	public final static String ACCESS_KEY = "ACCESS_KEY";
	public final static String SECRET_KEY = "SECRET_KEY";

	public DynamoDBEntityManager(String host, String port, String username,
			String password) {
		Map<String, String> properties = new HashMap<String, String>();
		properties.put(HOST, host);
		properties.put(PORT, port);
		properties.put(ACCESS_KEY, username);
		properties.put(SECRET_KEY, password);
		
		setUp(properties);
	}

	@Override
	protected void setUp(Map<String, String> properties) {
		if (dynamoDB != null) {
			return;
		}

		dynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials(
				properties.get(ACCESS_KEY), properties.get(SECRET_KEY)));
		if (properties.get(HOST) != null) {
			String url = "http://" + properties.get(HOST);
			if (properties.get(PORT) != null) {
				url = url + ":" + properties.get(PORT);
			}
			dynamoDB.setEndpoint("http://" + properties.get(HOST) + ":"
					+ properties.get(PORT));
		}
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
