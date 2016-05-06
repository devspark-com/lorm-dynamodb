package org.devspark.aws.lorm.dynamodb.it.schema;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.devspark.aws.lorm.EntityManager;
import org.devspark.aws.lorm.SchemaSupport;
import org.devspark.aws.lorm.dynamodb.DynamoDBEntityManager;
import org.devspark.aws.lorm.mapping.EntityToItemMapper;
import org.devspark.aws.lorm.mapping.EntityToItemMapperImpl;
import org.devspark.aws.lorm.mapping.ItemToEntityMapper;
import org.devspark.aws.lorm.mapping.ItemToEntityMapperImpl;
import org.devspark.aws.lorm.schema.validation.EntitySchemaSupport;
import org.junit.Assert;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class BaseIntegrationTest {
    private static final String PORT = System.getProperty("dynamodb.port");
    private static final String HOST = System.getProperty("dynamodb.host");

    private final static Log LOG = LogFactory.getLog(BaseIntegrationTest.class);
    
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

    protected void deleteTable(String tableName) {
        deleteTable(tableName, 30000);
    }

    protected void deleteTable(String tableName, int maxWait) {
        if (maxWait <= 0) {
            throw new IllegalStateException(
                    "Table " + tableName + " could not be deleted");
        }

        try {
            dynamoDB.describeTable(tableName).getTable();
            dynamoDB.deleteTable(tableName);
        } catch (ResourceNotFoundException ex) {
        } catch (ResourceInUseException ex) {
            LOG.warn("Could not delete [" + tableName
                    + "] table, because it is being used" 
                    + " . Will try again in 3 second");

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {

            }

            deleteTable(tableName, maxWait - 3000);
        }

        // was it really deleted ?
        TableDescription tableDesc = null;
        try {
            tableDesc = dynamoDB.describeTable(tableName).getTable();
        } catch (ResourceNotFoundException ex) {

        }

        if (tableDesc != null) {
            LOG.warn("Going to try again to delete the [" + tableName + "] table. "
                    + "Description is still there.");
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {

            }
            
            
            deleteTable(tableName, maxWait - 3000);
        }
    }

    protected <T> void addToEntityManager(Class<T> entityClass) {
        EntityToItemMapper entityToItemMapper = new EntityToItemMapperImpl<T>(
                entityClass);
        EntitySchemaSupport entitySchemaSupport = new EntityToItemMapperImpl<T>(
                entityClass);
        ItemToEntityMapper<T> itemToEntityMapper = new ItemToEntityMapperImpl<T>(
                entityClass, entityManager);

        entityManager.addEntity(entityClass, entityToItemMapper, itemToEntityMapper,
                entitySchemaSupport);
    }

    @SuppressWarnings("unchecked")
    protected <T> void setupSchemaForEntity(Class<T> entityClass) {
        SchemaSupport<T> repository = (SchemaSupport<T>) entityManager
                .getRepository(entityClass);
     //   Assert.assertTrue("delete table", repository.deleteTable());
        repository.syncToSchema(true, true, true);
    }
}
