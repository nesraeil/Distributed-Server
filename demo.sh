exec > >(tee "output.log") 2>&1
#Step 1
echo
mvn clean test

#Step 2
arr=()
#0 is the gateway, 7 is the leader
for i in {0..7}
do
	java -cp ./target/classes edu.yu.cs.fall2019.intro_to_distributed.Driver $i &
	arr+=("$!")
done

#Step 3
echo
HTTPD=`curl -A "" -sL --connect-timeout 3 -w "%{http_code}\n" "http://localhost:9999/getleader" -o /dev/null`
curl -s http://localhost:9999/getleader
until [ "$HTTPD" == "200" ]; do
    sleep 3
    HTTPD=`curl -A "" -sL --connect-timeout 3 -w "%{http_code}\n" "http://localhost:9999/getleader" -o /dev/null`
    printf "\n"
    curl -s http://localhost:9999/getleader
done


#Step 4
echo
for i in {1..9}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl -s http://localhost:9999/compileandrun -d "$string"
done

#Step 5
echo
echo "killing server 3"
kill -9 "${arr[3]}"
sleep 7s

HTTPD=`curl -A "" -sL --connect-timeout 3 -w "%{http_code}\n" "http://localhost:9999/getleader" -o /dev/null`
curl -s http://localhost:9999/getleader
until [ "$HTTPD" == "200" ]; do
    sleep 3
    HTTPD=`curl -A "" -sL --connect-timeout 3 -w "%{http_code}\n" "http://localhost:9999/getleader" -o /dev/null`
    printf "\n"
    curl -s http://localhost:9999/getleader
done

#Step 6a
echo "Killing leader"
kill -9 "${arr[7]}"
sleep 0.5s

#6b
echo
pids=()
for i in {1..9}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl -s http://localhost:9999/compileandrun -d "$string" & 
	pids+=($!)
done

#Step 7a
echo
sleep 7s

HTTPD=`curl -A "" -sL --connect-timeout 3 -w "%{http_code}\n" "http://localhost:9999/getleader" -o /dev/null`
curl -s http://localhost:9999/getleader
until [ "$HTTPD" == "200" ]; do
    sleep 3
    HTTPD=`curl -A "" -sL --connect-timeout 3 -w "%{http_code}\n" "http://localhost:9999/getleader" -o /dev/null`
    printf "\n"
    curl -s http://localhost:9999/getleader
done

#Step 7b
echo
echo "Waiting for 9 processes to finish"
for i in "${pids[@]}"
do
	if ps "$i" > /dev/null 
	then
		wait "$i"
	fi
	echo "Process $i is finished"
done


# Step 8
echo
for i in {1..2}
do
	string='public class HelloWorld {public void run() {System.out.print("Hello System.out world '$i'!\n");System.err.print("Hello System.err world '$i'!\n");}}'
	echo $string
	curl -s http://localhost:9999/compileandrun -d "$string"
done


# Step 9
echo
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
