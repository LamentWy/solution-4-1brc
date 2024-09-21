#!/usr/bin/env bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

echo "switch to java 21.0.3-oracle in this shell..."
sdk use java 21.0.3-oracle 1>&2

JAVA_OPTS="--enable-preview --add-modules=jdk.incubator.vector"
java $JAVA_OPTS --class-path app/build/libs/app.jar com.z.lament.obrc.advance.CopySolution
