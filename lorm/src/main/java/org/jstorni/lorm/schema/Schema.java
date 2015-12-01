package org.jstorni.lorm.schema;

import java.util.Set;

public class Schema {
	private final Set<EntitySchema> entitySchemas;

	public Schema(Set<EntitySchema> entitySchemas) {
		super();
		this.entitySchemas = entitySchemas;
	}

	public Set<EntitySchema> getEntitySchemas() {
		return entitySchemas;
	}

}
