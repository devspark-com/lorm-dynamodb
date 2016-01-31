package org.devspark.aws.lorm.test.model.embedded;

import javax.persistence.Embeddable;

@Embeddable
public class DeepEmbedded {
    private String embeddedField;

    public String getEmbeddedField() {
	return embeddedField;
    }

    public void setEmbeddedField(String embeddedField) {
	this.embeddedField = embeddedField;
    }

}
