#!/bin/sh

LIBS="user/appengine-api-1.0-sdk-*.jar \
    user/appengine-api-labs-*.jar \
    impl/appengine-api.jar \
    impl/appengine-local-runtime.jar \
    impl/appengine-api-stubs.jar"
      
usage() {
    echo usage: $0 appengine-dir
    echo copies the needed library from an extracted App Engine SDK directory
    exit 1
}

[ $# -ne 1 ] && usage

GAE_DIR=$1

[ -d lib ] || mkdir lib

for i in $LIBS
do
    cp $GAE_DIR/lib/$i lib
done
