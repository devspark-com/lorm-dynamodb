package org.jstorni.lorm.test.model.embedded;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class SampleEntity {
	private String someRandomField;
	@Id
	private String id;

	@Embedded
	private SampleEmbeddable embedded;

	public String getSomeRandomField() {
		return someRandomField;
	}

	public void setSomeRandomField(String someRandomField) {
		this.someRandomField = someRandomField;
	}

	public SampleEmbeddable getEmbedded() {
		return embedded;
	}

	public void setEmbedded(SampleEmbeddable embedded) {
		this.embedded = embedded;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
