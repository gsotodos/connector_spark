#!/bin/bash
set -x

export JAVA_INC=/usr/lib/jvm/java-11-openjdk-amd64/include
export XPN_INC=/home/lab/src/xpn/include
export XPN_SRC=/home/lab/src/xpn/src
export CLASSPATH=$CLASSPATH:/home/lab/connector_spark2/src/main/java

javac -h . main/java/org/expand/jni/ExpandToPosix.java

cc -c -fPIC -I$JAVA_INC -I$JAVA_INC/linux -I$XPN_INC/xpn_client/ -O2 -Wall -D_REENTRANT -DPOSIX_THREADS -DHAVE_CONFIG_H org_expand_jni_ExpandToPosix.c

cc -shared -o libexpandtoposix.so org_expand_jni_ExpandToPosix.o -L$XPN_SRC/base/ -L$XPN_SRC/xpn_client -L/home/lab/bin/mpich/lib -L/home/lab/bin/xpn/lib/ -lxpn -lmpi -lpthread -ldl

rm *.o

