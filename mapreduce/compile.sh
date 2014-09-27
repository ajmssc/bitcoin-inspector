#!/bin/bash

## compile script


build_dir=build
mkdir -p $build_dir
rm -rf $build_dir/*

#classpath="$(hadoop classpath):$(hbase classpath)"
classpath="$(hadoop classpath):$(hbase classpath)"
#echo $classpath
protoc --java_out=src/main/java/ src/main/protobuf/bitcoin.proto
javac -d $build_dir  -sourcepath src -classpath "$classpath"  $(find src -name "*.java")
#-Xlint:deprecation 




rm  -f a.jar
jar cf a.jar -C $build_dir .

rm -rf $build_dir
