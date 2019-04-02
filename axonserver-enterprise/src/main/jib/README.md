# Docker setup

**You can add arbitrary, non-classpath files to the image by placing them in a `src/main/jib` directory**
I this case `axoniq.license` will be added to the image root.

[https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin#adding-arbitrary-files-to-the-image](https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin#adding-arbitrary-files-to-the-image)

## Prerequisite

Install [Docker Desktop](https://www.docker.com/products/docker-desktop) and run it.

## Build the Docker images

Build the application (maven modules) image with [Jib](https://github.com/GoogleContainerTools/jib) directly to a local Docker daemon. 'Jib' uses the `docker` command line tool and requires that you have docker available on your PATH.

From parent/root folder:
```bash
$ mvn install jib:dockerBuild
```

> 'Jib' separates your application into multiple layers, splitting dependencies from classes. Now you don’t have to wait for Docker to rebuild your entire Java application - just deploy the layers that changed.

