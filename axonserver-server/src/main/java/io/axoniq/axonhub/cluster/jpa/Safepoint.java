package io.axoniq.axonhub.cluster.jpa;

import io.axoniq.axonhub.KeepNames;

import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;

/**
 * Author: marc
 */
@Entity
@IdClass(Safepoint.SafepointKey.class)
public class Safepoint {
    @Id
    private String context;
    @Id
    private String type;
    private long token;

    public Safepoint() {}
    public Safepoint(String type, String context, long token) {
        this.type = type;
        this.context = context;
        this.token = token;
    }

    public String getContext() {
        return context;
    }

    public long getToken() {
        return token;
    }

    public String getType() {
        return type;
    }

    public boolean isEvent() {
        return "Event".equals(type);
    }

    @KeepNames
    public static class SafepointKey implements Serializable {
        private String context;
        private String type;

        public SafepointKey() {
        }

        public SafepointKey(String context, String type) {
            this.context = context;
            this.type = type;
        }

        public String getContext() {
            return context;
        }

        public String getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SafepointKey that = (SafepointKey) o;

            if (!context.equals(that.context)) return false;
            return type.equals(that.type);
        }

        @Override
        public int hashCode() {
            int result = context.hashCode();
            result = 31 * result + type.hashCode();
            return result;
        }
    }
}
