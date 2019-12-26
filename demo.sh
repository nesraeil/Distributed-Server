#!/bin/bash
#mvn clean compile
arr=( )
for i in {0..8}
do
	java -cp target/classes edu.yu.cs.fall2019.intro_to_distributed.Driver $i & arr+=("$!")
done

sleep 10s

for i in {0..10}
do
	string=public class HelloWorld {public void run() {System.out.print("Hello System.out world!\n");System.err.print("Hello System.err world!\n");}}
    curl http://localhost:8010/compileandrun -d string
done

pkill java
