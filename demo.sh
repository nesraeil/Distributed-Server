exec > >(tee "output.txt") 2>&1
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
sleep 2s
curl -s http://localhost:9999/getleader

#Step 4
for i in {0..8}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl -s http://localhost:9999/compileandrun -d "$string"
done

#Step 5
echo "killing server 3"
kill "${arr[3]}"
sleep 7s
curl -s http://localhost:9999/getleader

#Step 6
pids=()
for i in {0..8}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl -s http://localhost:9999/compileandrun -d "$string" &
	pids+=($!)
done

#Step 7
sleep 1s
echo "Killing leader"
kill -9 "${arr[7]}"
sleep 7s
curl -s http://localhost:9999/getleader

# Step 8
for i in {0..1}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl -s http://localhost:9999/compileandrun -d "$string"
done


# Step 9
echo "Making sure the 9 processes no longer exist"
for i in "${pids[@]}"
do
	# if ps "$i" > /dev/null &
	# then
	echo "Killing PID$i"
	kill -9 "$i"
	# else 
		# echo "Process $i doesn't exist"
	# fi
done

# Step 10
#All of the "getgossip" ports are one port off from the server's actual ports
#This is because it is not possible to open a HttpServer for incoming http requests 
#while the TCP server is running for communication between the servers. This is also
#why the gateway's external facing server is 9999 and its peerserver facing one is 8000

#Gateway
curl -s http://localhost:9999/getgossip
#Server 1
curl -s http://localhost:8011/getgossip
#Server 2
curl -s http://localhost:8021/getgossip
#Server 4
curl -s http://localhost:8041/getgossip
#Server 5
curl -s http://localhost:8051/getgossip
#Server 6
curl -s http://localhost:8061/getgossip

pkill java
