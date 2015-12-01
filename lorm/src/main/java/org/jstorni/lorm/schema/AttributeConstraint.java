package org.jstorni.lorm.schema;

public class AttributeConstraint {
	private final AttributeConstraintType type;
	private final Object value;

	public AttributeConstraint(AttributeConstraintType type, Object value) {
		super();
		this.type = type;
		this.value = value;
	}

	public static AttributeConstraint buildPrimaryKeyConstraint() {
		return new AttributeConstraint(AttributeConstraintType.PRIMARY_KEY,
				null);
	}

	public AttributeConstraintType getType() {
		return type;
	}

	public Object getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		AttributeConstraint other = (AttributeConstraint) obj;
		if (type != other.type)
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}
