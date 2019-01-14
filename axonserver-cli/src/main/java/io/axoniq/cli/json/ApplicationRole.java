package io.axoniq.cli.json;

import java.util.Date;

/**
 * @author Marc Gathier
 */
public class ApplicationRole {
    private String role;
    private String context;
    private Date endDate;

    public ApplicationRole() {
    }

    public ApplicationRole(String role, Date endDate) {
        if( role.contains("@")) {
            String[] parts = role.split("@", 2);
            this.role = parts[0];
            this.context = parts[1];
        } else {
            this.role = role;
            this.context = "default";
        }
        this.endDate = endDate;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }
}
