package org.jstorni.lorm;

import org.jstorni.lorm.mapping.EntityToItemMapper;
import org.jstorni.lorm.mapping.ItemToEntityMapper;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;

public interface EntityManager {

	<T> void addEntity(Class<T> entityClass,
			EntityToItemMapper entityToItemMapper,
			ItemToEntityMapper<T> itemToEntityMapper,
			EntitySchemaSupport entitySchemaSupport);

	<T> Repository<T> getRepository(Class<T> entityClass);

}
