package io.axoniq.axonserver.localstorage.query.result;

import com.fasterxml.jackson.annotation.JsonValue;
import io.axoniq.axonserver.KeepNames;
import io.axoniq.axonserver.localstorage.query.ExpressionResult;
import net.minidev.json.JSONObject;

/**
 * @author Marc Gathier
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
