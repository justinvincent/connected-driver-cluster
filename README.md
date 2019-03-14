# connected-driver-cluster

# Overview

Connecting drivers to their cars can create a better driving experience, improve driver safety and create customer enthusiasm. By combining real time monitoring with analytics Automotive companies can collect actionable information from drivers that can be used to assist, inform and enhance the driving experience.

Using on board data collection to analyze driver patterns and overall vehicle vitales, car makers can push back service maintenance alerts and provide positive feedback to drivers via driver dashboards and mobile applications. The ability to collect, create continous models and analyze real time data from connected cars, is the challenge. 

By Leveraging MapR to collect all data and run continuous models, when can then publish this data to a backend mobile application database that will push changes and updates to the application. Doing this will deliver the intelligent feedback and actionable information to drivers in the form of alerts, car vitals, driving patterns and more. 

# Connect Driver Demonstration Components

1) Dataset: Includes trips from ? number of vehicles taking X number of trips 
2) Java Producer:"Ingestor" produces messages to a mapr stream   
3) Java Consumer:"Transformer" consumes and transforms dataset 
4) MapR Data Platform 6.1 
5) MapR Data Science Refinery (Zeppelin Notebook) 
6) Google Firebase
7) Connected Driver Mobile Application 



# Architecture 
![Data Pipeline Process](https://github.com/auddye/connected-driver-cluster/blob/working/images/ConnectedDriverArchitecture.png)

# Setup

**MapR Data Platform** 

Deploy MapR Data Platform version 6.1 

We will be creating an obd volume with the following 

MapR DB JSON Tables:  

-obd_raw_table
-obd_transformed
-obd_messages

MapR Event Store for Apahce Kafka stream and topic: 

-obd_msg_stream
-obd_msg_stream

From one of the nodes: 
```
git clone https://github.com/mpojeda84/connected-driver-cluster.git
./2-recreate-volume.sh
./3-create.sh
```

If you need to clean up the cluster run the following script to start over: 
If you would like this script to delete the firebase database use a text editor and paste in the link to your firebase database in the curl command
```
./1-delete.sh
```



**Edge Node Programs **

You can choose to run commands from the cluster or set up an edge node installed with the MapR Client. 

```
yum install maven
git clone https://github.com/mpojeda84/connected-driver-cluster.git
cd consumer/ingestor
mvn clean package
cd ../transformer
mvn clean package
```

** Google Firebase**

Log into Google Cloud 

https://firebase.google.com/products/

Click **Get Started**  
Click **Add Project**  
Name your project and click "Create project"  
Development Panel > Database > **Create database**     
Create "Realtime Database"   
Select **"Start in test mode"**  
Click **Enable**  
Add a field **Name: car, Value: test**  

Navigate to "Your Apps" 
Select **</>** for HTML
You will see **"Add Firebase to your web app"**
Copy this information and save it, it will be used in our Firebase.js code 


**Mobile Application** 

Your laptop will need the following installed: 
-Node.js
-XCode
-Maven
https://linuxize.com/post/how-to-install-apache-maven-on-centos-7/
put in opt directory; update your .bashrc PATH variable to include:
export PATH=/opt/apache-maven-3.6.0/bin:$PATH


Node.js : Download from here https://nodejs.org/en/download/
```
npm install -g expo-cli
```


On your personal computer or where you will be simulating the mobile application 
```
git clone https://github.com/mpojeda84/connected-car-client.git
cd firebase/
#install required node.js libraries
npm i
vi Firebase.js > paste in credentials 
```

We are now ready to start up our mobile application 
```
cd firebase 
mvn clean package  
npm install
expo start 
```

Keep hitting enter to get through the defaults 
When the Tunnel is ready hit “i” for iOS, this will launch the simulator 

![Tunnel launches](https://github.com/auddye/connected-driver-cluster/blob/working/images/tunnel.png)

Make sure you are using the correct iOS version 
XCode: window -> devices and simulators > components > simulator > iOS 12.0 Simulator 


# Running the Demonstration 

From the Edge Node, or from wherever you compiled your jar packages to run

**Establish MapR to Firebase Connection**
-t is the transformed data table we will be using to populate our firebase database. This is a complete path to the table. 
-f The link to your database 
-d messages per milliseconds (example: 200 , means 1 message every 200 milliseconds) 

![Firebase Link](https://github.com/auddye/connected-driver-cluster/blob/working/images/Firebaselink.png)


```
java -jar /mapr/my.cluster.com/user/mapr/connected-driver-cluster/firebase/target/connected-driver-firebase-2.0-SNAPSHOT.jar -t /mapr/my.cluster.com/obd/obd_transformed -f https://connected-driver-69921.firebaseio.com -d 200 -m /obd/obd_messages

```


**Start Ingesting and Transforming the Data**

Run the ingestor
```
/opt/mapr/spark/spark-2.3.1/bin/spark-submit --master yarn --deploy-mode client /mapr/my.cluster.com/user/mapr/connected-driver-cluster/consumers/ingestor/target/connected-driver-ingestor-2.0-SNAPSHOT.jar -n "/mapr/my.cluster.com/obd/obd_msg_stream:obd_msg" -t "/mapr/my.cluster.com/obd/obd_raw_table"
```

Run the transformer
```
/opt/mapr/spark/spark-2.3.1/bin/spark-submit --master yarn --deploy-mode client --num-executors 3 --executor-memory 1g  /mapr/my.cluster.com/user/mapr/connected-driver-cluster/consumers/connected-driver-transformer-2.0-SNAPSHOT.jar -h /obd/obd_checkpoints -n /obd/obd_msg_stream:obd_msg -r /obd/obd_transformed -o "2019-01-28 0:55:08"
```

Navigate to the MapR Cluster MCS 

https://maprcluster.com:8443 

View the volumes and tables for and notice the volume size change as the data is streamed into the platform. 



