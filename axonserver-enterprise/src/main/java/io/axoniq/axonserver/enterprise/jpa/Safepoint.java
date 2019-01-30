package io.axoniq.axonserver.enterprise.jpa;

import io.axoniq.axonserver.KeepNames;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;

/**
 * @author Marc Gathier
 */
@Entity
@IdClass(Safepoint.SafepointKey.class)
public class Safepoint {
    @Id
    private String context;
    @Id
    private String type;

    @Column(name = "token")  // for retro compatibility
    private long safePoint;
    /**
     * The generation of the master this node last synchronize with
     */
    private long generation;

    public Safepoint() {}

    public Safepoint(String type, String context) {
        this(type, context, 0, 0);
    }
    public Safepoint(String type, String context, long safePoint, long generation) {
        this.type = type;
        this.context = context;
        this.safePoint = safePoint;
        this.generation = generation;
    }

    public String getContext() {
        return context;
    }

    public long safePoint() {
        return safePoint;
    }

    public void setSafePoint(long safePoint) {
        this.safePoint = safePoint;
    }

    public String getType() {
        return type;
    }

    public long generation() {
        return generation;
    }

    public void increaseGeneration() {
        this.generation++;
    }

    public void setGeneration(long generation) {
        this.generation = generation;
    }

    public boolean isEvent() {
        return "event".equalsIgnoreCase(type);
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
