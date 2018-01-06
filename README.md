# trellis-rosid-file

**NOTE**: this project has been migrated into the [Trellis/Rosid repository](https://github.com/trellis-ldp/trellis-rosid).

A file-based implementation of the Trellis API, based on Kafka and a distributed data store.

The basic principle behind this implementation is to represent resource state as a stream of (re-playable) operations.

## Building

This code requires Java 8 and can be built with Gradle:

    ./gradlew install
