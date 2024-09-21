#!/usr/bin/env bash
# copy from prepare_thomaswue.sh

source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.2-graal 1>&2

if [ ! -f app/build/libs/CopySolutionImage ]; then

    # Performance tuning flags, optimization level 3, maximum inlining exploration, and compile for the architecture where the native image is generated.
    NATIVE_IMAGE_OPTS="-O3 -H:TuneInlinerExploration=1 -march=native"

    # Need to enable preview for accessing the raw address of the foreign memory access API.
    # Initializing the Scanner to make sure the unsafe access object is known as a non-null compile time constant.
    NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS --enable-preview --initialize-at-build-time=com.z.lament.obrc.advance.CopySolution\$MemoryReader"

    # There is no need for garbage collection and therefore also no safepoints required.
    NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS --gc=epsilon -H:-GenLoopSafepoints"

    # Uncomment the following line for outputting the compiler graph to the IdealGraphVisualizer
    # NATIVE_IMAGE_OPTS="$NATIVE_IMAGE_OPTS -H:MethodFilter=CopySolution.* -H:Dump=:2 -H:PrintGraph=Network"

    native-image $NATIVE_IMAGE_OPTS -cp app/build/libs/app.jar -o app/build/libs/CopySolutionImage com.z.lament.obrc.advance.CopySolution
fi
