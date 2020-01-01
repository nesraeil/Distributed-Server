#!/bin/bash
mvn clean compile
arr=()
for i in {0..7}
do
	java -cp ./target/classes edu.yu.cs.fall2019.intro_to_distributed.Driver $i & arr+=("$!")
done

sleep 5s

for i in {0..6}
do
	
	curl http://localhost:9999/compileandrun -d $'public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
done

sleep 10s

pkill java
