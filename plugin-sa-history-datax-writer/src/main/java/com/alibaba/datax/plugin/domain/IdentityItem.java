package com.alibaba.datax.plugin.domain;

import java.io.Serializable;

public class IdentityItem implements Serializable {

    private static final long serialVersionUID = -4899424017058330965L;

    private String idName;

    private String column;

    private boolean isColumn = true;

    public String getIdName() {
        return idName;
    }

    public void setIdName(String idName) {
        this.idName = idName;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public boolean isColumn() {
        return isColumn;
    }

    public void setColumn(boolean column) {
        isColumn = column;
    }

}
