#!/usr/bin/env bash
if [ -f app/build/libs/CopySolutionImage ]; then
    echo "Found executable image 'app/build/libs/CopySolutionImage', start running..." 1>&2
    app/build/libs/CopySolutionImage
else
    echo " Image not found, build this project and run 'sh prepare_copysolution_4_graalVM.sh' first." 1>&2
fi
