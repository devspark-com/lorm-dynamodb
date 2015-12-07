package org.jstorni.lorm.mapping;

import java.util.Map;

import org.jstorni.lorm.schema.AttributeDefinition;

public interface EntityToItemMapper {

	Map<AttributeDefinition, Object> map(Object entity);

}
