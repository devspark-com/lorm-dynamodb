package org.jstorni.lorm.schema;

import java.util.Set;

public class AttributeDefinition {

	private final String name;
	private final AttributeType type;
	private final Set<AttributeConstraint> constraints;

	public AttributeDefinition(String name, AttributeType type,
			Set<AttributeConstraint> constraints) {
		super();
		this.name = name;
		this.type = type;
		this.constraints = constraints;
	}

	public String getName() {
		return name;
	}

	public AttributeType getType() {
		return type;
	}

	public Set<AttributeConstraint> getConstraints() {
		return constraints;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AttributeDefinition other = (AttributeDefinition) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

}
