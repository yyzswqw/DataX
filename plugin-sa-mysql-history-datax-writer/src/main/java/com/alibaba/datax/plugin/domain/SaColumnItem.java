package com.alibaba.datax.plugin.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SaColumnItem implements Serializable {

    private static final long serialVersionUID = -1306205173606479993L;

    private Integer index;

    private String name;

    /**
     * update时，作为where条件的列也需要更新为其他值时配置该项
     */
    private String updateNewValueCol;

    private Boolean ifNullGiveUp = false;

    /**
     * 是否排除该字段的导入
     */
    private Boolean exclude = false;

    private List<DataConverter> dataConverters;

}
