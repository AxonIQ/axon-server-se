package io.axoniq.axonserver.eventstore.transformation.clean.transformations;

import io.axoniq.axonserver.eventstore.transformation.clean.TransformationIdentifier;
import io.axoniq.axonserver.eventstore.transformation.clean.TransformationsToBeCleaned;
import io.axoniq.axonserver.eventstore.transformation.requestprocessor.TransformationBaseStorageProvider;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.nio.file.Paths;

/**
 * @author Sara Pellegrini
 * @since 2023.0.0
 */
public class FileSystemTransformations implements TransformationsToBeCleaned {

    private final File baseStorage;

    public FileSystemTransformations(TransformationBaseStorageProvider transformationBaseStorage) {
        this(Paths.get(transformationBaseStorage.storageLocation()).toFile());
    }

    public FileSystemTransformations(File baseStorage) {
        this.baseStorage = baseStorage;
    }


    @Override
    public Flux<TransformationIdentifier> get() {
        return Mono.fromSupplier(() -> baseStorage.listFiles(File::isDirectory))
                   .flatMapMany(Flux::fromArray)
                   .flatMap(contextDirectory -> Mono.fromSupplier(() -> contextDirectory.listFiles(File::isDirectory))
                                                    .flatMapMany(Flux::fromArray)
                                                    .map(storeDirectory -> new DefaultTransformationIdentifier(
                                                            contextDirectory.getName(),
                                                            storeDirectory.getName()
                                                    )));
    }
}
