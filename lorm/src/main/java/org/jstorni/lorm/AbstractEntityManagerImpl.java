package org.jstorni.lorm;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.jstorni.lorm.mapping.EntityToItemMapper;
import org.jstorni.lorm.mapping.ItemToEntityMapper;
import org.jstorni.lorm.schema.validation.EntitySchemaSupport;

public abstract class AbstractEntityManagerImpl implements EntityManager {

	private final Map<Class<?>, Repository<?>> repositories;

	public AbstractEntityManagerImpl() {
		repositories = Collections
				.synchronizedMap(new HashMap<Class<?>, Repository<?>>());
	}

	public AbstractEntityManagerImpl(Map<String, String> properties) {
		this();
		setUp(properties);
	}

	protected void setUp(Map<String, String> properties) {

	}

	protected Map<Class<?>, Repository<?>> getAllRepositories() {
		return repositories;
	}

	@Override
	public abstract <T> void addEntity(Class<T> entityClass,
			EntityToItemMapper entityToItemMapper,
			ItemToEntityMapper<T> itemToEntityMapper,
			EntitySchemaSupport entitySchemaSupport);

	@SuppressWarnings("unchecked")
	@Override
	public <T> Repository<T> getRepository(Class<T> entityClass) {
		return (Repository<T>) repositories.get(entityClass);
	}

}
