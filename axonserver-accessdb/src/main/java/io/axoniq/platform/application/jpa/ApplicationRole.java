package io.axoniq.platform.application.jpa;


import javax.persistence.*;
import java.util.Date;

/**
 * Created by marc on 7/13/2017.
 */
@Entity
public class ApplicationRole {
    @Id
    @GeneratedValue
    private Long id;

    private String role;

    private String context;

    @Temporal(TemporalType.TIMESTAMP)
    private Date endDate;

    public ApplicationRole() {
    }

    public ApplicationRole(String role, String context, Date endDate) {
        this.role = role;
        this.endDate = endDate;
        this.context = context;
    }

    public String getRole() {
        return role;
    }

    public Date getEndDate() {
        return endDate;
    }

    public String getContext() {
        return context;
    }
}
