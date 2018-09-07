package io.axoniq.axonserver.grpc;

/**
 * Created by Sara Pellegrini on 17/08/2018.
 * sara.pellegrini@gmail.com
 */
public interface Converter<A,B> {

    B map(A strategy);

    A unmap(B strategy);
}
