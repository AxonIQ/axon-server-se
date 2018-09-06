package io.axoniq.axonhub.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.localstorage.query.ExpressionResult;
import net.minidev.json.JSONObject;

/**
 * Author: marc
 */
@KeepNames
public class JSONExpressionResult implements AbstractMapExpressionResult  {
    private final JSONObject value;

    public JSONExpressionResult(JSONObject value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public int compareTo( ExpressionResult o) {
        return value.toJSONString().compareTo(o.toString());
    }

    @Override
    @JsonValue
    public String toString() {
        return value.toJSONString();
    }

    @Override
    public Iterable<String> getColumnNames() {
        return value.keySet();
    }

}
