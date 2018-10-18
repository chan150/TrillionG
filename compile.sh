#!/usr/bin/env bash
sbt/bin/sbt clean
sbt/bin/sbt package
cp target/scala*/*.jar TrillionG.jar