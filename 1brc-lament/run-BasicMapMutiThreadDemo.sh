#!/usr/bin/env zsh

JAVA_OPTS="--add-modules=jdk.incubator.vector"
java $JAVA_OPTS --class-path app/build/libs/app.jar com.z.lament.obrc.mutithread.BasicMapMutiThreadDemo
