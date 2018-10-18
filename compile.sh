#!/usr/bin/env bash
sbt/bin/sbt package
cp target/*/*.jar TrillionG.jar