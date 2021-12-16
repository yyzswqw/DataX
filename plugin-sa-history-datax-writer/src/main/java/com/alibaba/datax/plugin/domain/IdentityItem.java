package com.alibaba.datax.plugin.domain;

import java.io.Serializable;

public class IdentityItem implements Serializable {

    private static final long serialVersionUID = -4899424017058330965L;

    private String idName;

    private String columnName;

    private boolean column = true;

    private boolean distinctId = false;

    public String getIdName() {
        return idName;
    }

    public void setIdName(String idName) {
        this.idName = idName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public boolean isColumn() {
        return column;
    }

    public void setColumn(boolean column) {
        this.column = column;
    }

    public boolean isDistinctId() {
        return distinctId;
    }

    public void setDistinctId(boolean distinctId) {
        this.distinctId = distinctId;
    }
}
