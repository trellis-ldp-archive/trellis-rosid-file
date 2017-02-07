# trellis-rosid

[![Build Status](https://travis-ci.org/trellis-ldp/trellis-rosid.png?branch=master)](https://travis-ci.org/trellis-ldp/trellis-rosid)

An implementation of the Trellis API, based on Kafka and a distributed data store.

The basic principle behind this implementation is to represent resource state as a stream of (re-playable) operations.

## Building

This code requires Java 8 and can be built with Gradle:

    ./gradlew install
