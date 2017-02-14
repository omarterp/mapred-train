#!/bin/bash

# to run this, provide the name of the java file without the extension
# example, ./make.sh TitleCount

#make sure env is set
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

#clean
rm -rf ./build/* ./$1.jar

#build jar
hadoop com.sun.tools.javac.Main $1.java -d build
jar -cvf $1.jar -C build/ ./