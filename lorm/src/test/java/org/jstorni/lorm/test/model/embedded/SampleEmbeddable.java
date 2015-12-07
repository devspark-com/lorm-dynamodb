package org.jstorni.lorm.test.model.embedded;

import javax.persistence.Embeddable;
import javax.persistence.Embedded;

@Embeddable
public class SampleEmbeddable {
	private String someField;

	@Embedded
	private SampleEmbeddable deepEmbedded;

	public String getSomeField() {
		return someField;
	}

	public void setSomeField(String someField) {
		this.someField = someField;
	}

	public SampleEmbeddable getDeepEmbedded() {
		return deepEmbedded;
	}

	public void setDeepEmbedded(SampleEmbeddable deepEmbedded) {
		this.deepEmbedded = deepEmbedded;
	}
}