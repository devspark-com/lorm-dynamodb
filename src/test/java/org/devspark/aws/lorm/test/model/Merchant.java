package org.devspark.aws.lorm.test.model;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import org.devspark.aws.lorm.test.model.BaseEntity;

@Entity
@Table(indexes = {@Index(columnList="name", unique=false)})
public class Merchant extends BaseEntity {

    @NotNull
    @Size(min = 8, max = 32)
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
