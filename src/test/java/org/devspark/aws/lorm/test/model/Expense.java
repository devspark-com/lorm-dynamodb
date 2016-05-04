package org.devspark.aws.lorm.test.model;

import java.math.BigDecimal;
import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Index;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

@Entity
@Table(indexes= {@Index(name="expense.merchant", columnList="merchant.id", unique=false)})
public class Expense extends BaseEntity {

    @NotNull
    @ManyToOne
    private Merchant merchant;

    @NotNull
    private BigDecimal amount;

    @NotNull
    private Date date;

    @NotNull
    @Size(min = 8, max = 128)
    private String description;

    @NotNull
    private ExpenseType expenseType;

    public Merchant getMerchant() {
        return merchant;
    }

    public void setMerchant(Merchant merchant) {
        this.merchant = merchant;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public ExpenseType getExpenseType() {
        return expenseType;
    }

    public void setExpenseType(ExpenseType expenseType) {
        this.expenseType = expenseType;
    }

}
