package org.jstorni.lorm;

import java.util.List;

public interface Repository<T> {

	T findOne(String id);

	// TODO add page support
	List<T> findAll();

	T save(T instance);

	List<T> save(List<T> instances);
	
	void deleteById(String id);
	
	void deleteById(List<String> ids);
	
}