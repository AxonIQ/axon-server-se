package io.axoniq.axonserver.localstorage.query;

import java.util.List;

/**
 * Author: marc
 */
public class QueryMetadata {
    private List<String> identifyingColumns;
    private List<String> sortColumns;

    public List<String> getIdentifyingColumns() {
        return identifyingColumns;
    }

    public void setIdentifyingColumns(List<String> identifyingColumns) {
        this.identifyingColumns = identifyingColumns;
    }

    public List<String> getSortColumns() {
        return sortColumns;
    }

    public void setSortColumns(List<String> sortColumns) {
        this.sortColumns = sortColumns;
    }
}
