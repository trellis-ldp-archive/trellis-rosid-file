# trellis-rosid-file

[![Build Status](https://travis-ci.org/trellis-ldp/trellis-rosid-file.png?branch=master)](https://travis-ci.org/trellis-ldp/trellis-rosid-file)
[![Build status](https://ci.appveyor.com/api/projects/status/w04a5ssibajvcuui?svg=true)](https://ci.appveyor.com/project/acoburn/trellis-rosid-file)
[![Coverage Status](https://coveralls.io/repos/github/trellis-ldp/trellis-rosid-file/badge.svg?branch=master)](https://coveralls.io/github/trellis-ldp/trellis-rosid-file?branch=master)


A file-based implementation of the Trellis API, based on Kafka and a distributed data store.

The basic principle behind this implementation is to represent resource state as a stream of (re-playable) operations.

## Building

This code requires Java 8 and can be built with Gradle:

    ./gradlew install
