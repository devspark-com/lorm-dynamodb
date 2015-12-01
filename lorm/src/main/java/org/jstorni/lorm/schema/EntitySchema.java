package org.jstorni.lorm.schema;

import java.util.Set;

public class EntitySchema {
	private final String name;
	private final Set<AttributeDefinition> attributes;

	public EntitySchema(String name, Set<AttributeDefinition> attributes) {
		super();
		this.name = name;
		this.attributes = attributes;
	}

	public String getName() {
		return name;
	}

	public Set<AttributeDefinition> getAttributes() {
		return attributes;
	}

}
