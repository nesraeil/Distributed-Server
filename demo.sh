#!/bin/bash
mvn clean compile
arr=()
#0 is the gateway, 7 is the leader
for i in {0..7}
do
	java -cp ./target/classes edu.yu.cs.fall2019.intro_to_distributed.Driver $i & arr+=("$!")
done

sleep 5s

curl http://localhost:9999/getleader

for i in {0..2}
do
	
	curl http://localhost:9999/compileandrun -d $'public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
done

echo "killing x in 8 secs"
sleep 8s

echo "killing 2"
kill "${arr[1]}"

sleep 8s

curl http://localhost:9999/getleader

# echo "killing server 3"
# kill "${arr[2]}"

# sleep 5s
# curl http://localhost:9999/getleader

# for i in {0..6}
# do
	
	# curl http://localhost:9999/compileandrun -d $'public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
# done

# sleep 100s

pkill java
