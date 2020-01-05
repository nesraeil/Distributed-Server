#Step 1
mvn test

#Step 2
arr=()
#0 is the gateway, 7 is the leader
for i in {0..7}
do
	java -cp ./target/classes edu.yu.cs.fall2019.intro_to_distributed.Driver $i & arr+=("$!")
done

#Step 3
sleep 1s
curl http://localhost:9999/getleader

#Step 4
for i in {0..8}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl http://localhost:9999/compileandrun -d "$string"
done

#Step 5
echo "killing server 3"
kill "${arr[3]}"
sleep 6s
curl http://localhost:9999/getleader

#Step 6
pids=()
for i in {0..8}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl http://localhost:9999/compileandrun -d "$string" &
	pids+=($!)
done

#Step 7
sleep 1s
echo "Killing leader"
kill -9 "${arr[7]}"
sleep 6
curl http://localhost:9999/getleader

# Step 8
for i in {0..1}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl http://localhost:9999/compileandrun -d "$string"
done


# Step 9
for i in "${pids[@]}"
do
	if ps "$i" > /dev/null
	then
	#This should never happen
		echo "Killing PID$i"
		kill -9 "$i"
	else 
		echo "PID$i no longer exists"
	fi
done

# Step 10
#Gateway
curl http://localhost:9999/getgossip
#Server 1
curl http://localhost:8011/getgossip
#Server 2
curl http://localhost:8021/getgossip
#Server 4
curl http://localhost:8041/getgossip
#Server 5
curl http://localhost:8051/getgossip
#Server 6
curl http://localhost:8061/getgossip

pkill java
