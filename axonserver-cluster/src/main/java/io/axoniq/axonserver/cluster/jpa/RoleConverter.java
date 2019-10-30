package io.axoniq.axonserver.cluster.jpa;

import io.axoniq.axonserver.cluster.util.RoleUtils;
import io.axoniq.axonserver.grpc.cluster.Role;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

/**
 * Maps enum Role to integer to store in database.
 * @author Marc Gathier
 * @since 4.3
 */
@Converter(autoApply = true)
public class RoleConverter implements AttributeConverter<Role, Integer> {

    @Override
    public Integer convertToDatabaseColumn(Role role) {
        return role.getNumber();
    }

    @Override
    public Role convertToEntityAttribute(Integer roleValue) {
        return RoleUtils.forNumber(roleValue);
    }
}
