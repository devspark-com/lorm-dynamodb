package org.devspark.aws.lorm.dynamodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import javax.persistence.Entity;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.devspark.aws.lorm.SchemaSupport;
import org.devspark.aws.lorm.exceptions.DataException;
import org.devspark.aws.lorm.exceptions.DataValidationException;
import org.devspark.aws.lorm.id.EntityIdHandler;
import org.devspark.aws.lorm.schema.AttributeConstraint;
import org.devspark.aws.lorm.schema.AttributeDefinition;
import org.devspark.aws.lorm.schema.AttributeType;
import org.devspark.aws.lorm.schema.EntitySchema;
import org.devspark.aws.lorm.schema.Index;
import org.devspark.aws.lorm.schema.validation.EntitySchemaSupport;
import org.devspark.aws.lorm.schema.validation.SchemaValidationError;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class DynamoDBSchemaSupport<T> implements SchemaSupport<T> {
    private final static Log LOG = LogFactory.getLog(DynamoDBSchemaSupport.class);

    private final EntityIdHandler idHandler;
    private Table table;
    private final DynamoDB dynamoDB;
    private final Class<T> entityClass;
    private final EntitySchemaSupport entitySchemaSupport;
    private final Set<Index> entityIndexes;
    private final long DELETE_TABLE_TIMEOUT = 60000;

    public DynamoDBSchemaSupport(DynamoDB dynamoDB,
            EntitySchemaSupport entitySchemaSupport, Class<T> entityClass) {
        idHandler = new EntityIdHandler(entityClass);
        this.dynamoDB = dynamoDB;
        this.entitySchemaSupport = entitySchemaSupport;
        this.entityClass = entityClass;
        table = getDynamoDbTable();
        entityIndexes = getIndexes();
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    private Table getDynamoDbTable() {
        Entity entityAnnotation = entityClass.getAnnotation(Entity.class);
        if (entityAnnotation == null) {
            throw new DataException("Not an entity: " + entityClass.getName());
        }

        String tableName = entityAnnotation.name() != null
                && !entityAnnotation.name().isEmpty()
                        ? entityAnnotation.name()
                        : entityClass.getName()
                                .substring(entityClass.getName().lastIndexOf('.') + 1)
                                .toLowerCase();

        return dynamoDB.getTable(tableName);
    }

    protected Table getTable() {
        return table;
    }

    protected DynamoDB getDynamoDB() {
        return dynamoDB;
    }

    protected EntityIdHandler getIdHandler() {
        return idHandler;
    }

    protected Set<Index> getEntityIndexes() {
        return entityIndexes;
    }

    @Override
    public boolean isValid(final List<SchemaValidationError> errors) {

        if (getTable().getDescription() == null) {
            try {
                getTable().describe();
            } catch (ResourceNotFoundException ex) {
                errors.add(
                        SchemaValidationError.buildTableError(getTable().getTableName(),
                                "Table not found: " + getTable().getTableName()));
                return false;
            }
        }

        if (getTable().getDescription().getAttributeDefinitions() == null
                || getTable().getDescription().getAttributeDefinitions().isEmpty()) {
            errors.add(SchemaValidationError.buildTableError(getTable().getTableName(),
                    "No attributes found in table: " + getTable().getTableName()));
            return false;
        }

        List<SchemaValidationError> validationErrors = entitySchemaSupport
                .validateSchema(getMapToEntitySchema());
        validationErrors.stream().filter(new Predicate<SchemaValidationError>() {

            @Override
            public boolean test(SchemaValidationError error) {
                Set<AttributeConstraint> attrConstraints = error.getAttributeDefinition()
                        .getConstraints();
                return attrConstraints != null && attrConstraints
                        .contains(AttributeConstraint.buildPrimaryKeyConstraint());
            }
        }).forEach(new Consumer<SchemaValidationError>() {

            @Override
            public void accept(SchemaValidationError error) {
                errors.add(error);
            }
        });

        return errors.isEmpty();
    }

    private EntitySchema getMapToEntitySchema() {
        Set<AttributeDefinition> attributes = new HashSet<AttributeDefinition>();

        TableDescription tableDesc = getTable().getDescription();
        List<com.amazonaws.services.dynamodbv2.model.AttributeDefinition> sourceAttrs = tableDesc
                .getAttributeDefinitions();
        for (com.amazonaws.services.dynamodbv2.model.AttributeDefinition attributeDefinition : sourceAttrs) {
            AttributeType attrType = getAttributeType(
                    attributeDefinition.getAttributeType());

            // TODO get constraints
            attributes.add(new AttributeDefinition(attributeDefinition.getAttributeName(),
                    attrType, null));
        }

        return new EntitySchema(tableDesc.getTableName(), attributes, entityIndexes);
    }

    private Set<Index> getIndexes() {
        Set<Index> indexes = new HashSet<>();

        javax.persistence.Table table = getEntityClass()
                .getAnnotation(javax.persistence.Table.class);
        if (table == null || table.indexes() == null || table.indexes().length == 0) {
            return indexes;
        }

        javax.persistence.Index[] tableIndexes = table.indexes();
        for (javax.persistence.Index tableIndex : tableIndexes) {
            List<String> fieldList = Arrays.asList(tableIndex.columnList().split(","));
            StringBuilder indexName = new StringBuilder();
            for (String fieldName : fieldList) {
                indexName.append(indexName.length() == 0 ? "." : "-")
                        .append(fieldName.trim());
            }

            Index index = new Index();
            index.setName(getTable().getTableName() + indexName.toString());
            index.setAttributeNames(fieldList);
            indexes.add(index);
        }

        return indexes;
    }

    private AttributeType getAttributeType(String scalarAttributeType) {
        AttributeType attrType;
        if (scalarAttributeType.equals(ScalarAttributeType.S.toString())) {
            attrType = AttributeType.STRING;
        } else if (scalarAttributeType.equals(ScalarAttributeType.N.toString())) {
            attrType = AttributeType.NUMBER;
        } else if (scalarAttributeType.equals(ScalarAttributeType.B.toString())) {
            attrType = AttributeType.BOOLEAN;
        } else {
            throw new DataValidationException(
                    "Unsupported Dynamo attribute type: " + scalarAttributeType);
        }

        return attrType;

    }

    protected AttributeType getAttributeType(Class<?> attributeClass) {
        // TODO add other supported types
        AttributeType attrType;
        if (Number.class.isAssignableFrom(attributeClass)) {
            attrType = AttributeType.NUMBER;
        } else if (String.class.equals(attributeClass)) {
            attrType = AttributeType.STRING;
        } else if (Boolean.class.equals(attributeClass)) {
            attrType = AttributeType.BOOLEAN;
        } else {
            throw new DataValidationException(
                    "Attribute type not supported: " + attributeClass.getName());
        }

        return attrType;

    }

    @Override
    public boolean syncToSchema(boolean deleteMissingFields,
            boolean createTableIfNotExists, boolean createReferences) {

        boolean isNewTable = false;

        try {
            getTable().describe();
        } catch (ResourceNotFoundException ex) {
            if (createTableIfNotExists) {
                isNewTable = true;
            } else {
                throw new DataValidationException(
                        "Table not found: " + getTable().getTableName());
            }
        }

        if (createTableIfNotExists && isNewTable) {

            // TODO assuming global secondary indexes, first approach will focus
            // on facilitate finding on ManyToOne relationships
            List<com.amazonaws.services.dynamodbv2.model.AttributeDefinition> indexAttrDefs = new ArrayList<com.amazonaws.services.dynamodbv2.model.AttributeDefinition>();
            List<GlobalSecondaryIndex> secondaryIndexes = new ArrayList<>();
            for (Index descriptorIndex : entityIndexes) {

                // TODO get parameters from configuration
                // TODO define projection attributes
                GlobalSecondaryIndex tableIndex = new GlobalSecondaryIndex()
                        .withIndexName(descriptorIndex.getName())
                        .withProvisionedThroughput(new ProvisionedThroughput()
                                .withReadCapacityUnits(100L).withWriteCapacityUnits(100L))
                        .withProjection(
                                new Projection().withProjectionType(ProjectionType.ALL));

                if (descriptorIndex.getAttributeNames() == null
                        || descriptorIndex.getAttributeNames().size() != 1) {
                    throw new DataValidationException("Error while creating index "
                            + descriptorIndex.getName() + " for table "
                            + getTable().getTableName()
                            + ". Reason: expected only one attribute for the index");
                }

                String attrNameToIndex = descriptorIndex.getAttributeNames().get(0);
                // TODO validate attribute

                ArrayList<KeySchemaElement> indexKeySchema = new ArrayList<KeySchemaElement>();
                indexKeySchema.add(new KeySchemaElement()
                        .withAttributeName(attrNameToIndex).withKeyType(KeyType.HASH));

                tableIndex.setKeySchema(indexKeySchema);

                secondaryIndexes.add(tableIndex);

                // TODO assuming index will be used for ManyToOne relationships,
                // were attribute to index is an id and ids are Strings
                indexAttrDefs
                        .add(new com.amazonaws.services.dynamodbv2.model.AttributeDefinition(
                                attrNameToIndex, ScalarAttributeType.S));
            }

            List<com.amazonaws.services.dynamodbv2.model.AttributeDefinition> attrDefs = new ArrayList<com.amazonaws.services.dynamodbv2.model.AttributeDefinition>();
            // TODO assuming String id
            attrDefs.add(new com.amazonaws.services.dynamodbv2.model.AttributeDefinition(
                    idHandler.getIdFieldName(), ScalarAttributeType.S));
            attrDefs.addAll(indexAttrDefs);

            List<KeySchemaElement> keys = new ArrayList<KeySchemaElement>();
            keys.add(new KeySchemaElement(idHandler.getIdFieldName(), KeyType.HASH));

            // TODO get this from configuration
            ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
            provisionedThroughput.setReadCapacityUnits(100L);
            provisionedThroughput.setWriteCapacityUnits(100L);

            CreateTableRequest createTableReq = new CreateTableRequest()
                    .withTableName(getTable().getTableName()).withKeySchema(keys);
            createTableReq.setAttributeDefinitions(attrDefs);
            createTableReq.setProvisionedThroughput(provisionedThroughput);
            if (!secondaryIndexes.isEmpty()) {
                createTableReq.withGlobalSecondaryIndexes(secondaryIndexes);
            }

            table = dynamoDB.createTable(createTableReq);
            try {
                table.waitForActive();
            } catch (InterruptedException ex) {
                throw new DataValidationException(
                        "Error while creating table: " + getTable().getTableName(), ex);
            }

        }

        if (!isNewTable) {
            List<SchemaValidationError> errors = new ArrayList<SchemaValidationError>();
            if (!isValid(errors)) {
                throw new DataValidationException(
                        "Table " + getTable().getTableName() + " is invalid");
            }
        }

        return true;
    }

    @Override
    public boolean deleteTable() {
        return deleteTable(getTable().getTableName(), DELETE_TABLE_TIMEOUT);
    }

    private boolean deleteTable(String tableName, long maxWait) {
        boolean deleted = false;

        if (maxWait <= 0) {
            throw new IllegalStateException(
                    "Table " + tableName + " could not be deleted");
        }

        TableDescription tableDesc = null;
        try {
            tableDesc = getTable().describe();
            getTable().delete();
            deleted = true;
        } catch (ResourceNotFoundException ex) {
            LOG.info("Resource not found " + getTable().getTableName()
                    + ". Assuming deleted");
            deleted = true;
        } catch (ResourceInUseException ex) {
            LOG.warn("Could not delete [" + tableName
                    + "] table, will try again in 5 seconds");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {

            }

            return deleteTable(tableName, maxWait - 5000);

        }

        // was it really deleted?
        try {
            tableDesc = getTable().describe();
        } catch (ResourceNotFoundException ex) {
            LOG.info("Table description not found " + getTable().getTableName()
                    + ". Assuming deleted");
            deleted = true;
        }

        if (tableDesc != null) {
            LOG.warn("Going to try again to delete the [" + tableName
                    + "] table in 5 seconds");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {

            }

            return deleteTable(tableName, maxWait - 5000);
        }

        return deleted;
    }

}
