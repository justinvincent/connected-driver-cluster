# connected-driver-cluster

# Overview

Connecting drivers to their cars can create a better driving experience, improve driver safety and create customer enthusiasm. By combining real time monitoring with analytics Automotive companies can collect actionable information from drivers that can be used to assist, inform and enhance the driving experience.

Using on board data collection to analyze driver patterns and overall vehicle vitales, car makers can push back service maintenance alerts and provide positive feedback to drivers via driver dashboards and mobile applications. The ability to collect, create continous models and analyze real time data from connected cars, is the challenge. 

By Leveraging MapR to collect all data and run continuous models, when can then publish this data to a backend mobile application database that will push changes and updates to the application. Doing this will deliver the intelligent feedback and actionable information to drivers in the form of alerts, car vitals, driving patterns and more. 

# Connect Driver Demonstration Components

1) Dataset: Includes trips from ? number of vehicles taking X number of trips 
2) Java Producer: produces messages to a mapr stream   
3) Java Consumer: consumes and transforms dataset 
4) MapR Data Platform 6.1 
5) MapR Data Science Refinery (Zeppelin Notebook) 
6) Google Firebase
7) Connected Driver Mobile Application 



# Architecture 
![Data Pipeline Process](https://github.com/auddye/connected-driver-cluster/blob/working/ConnectedDriverArchitecture.png)

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

**Edge Node Programs **

You can choose to run commands from the cluster or set up an edge node installed with the MapR Client. 

```
yum install maven
git clone https://github.com/mpojeda84/connected-driver-cluster.git
cd consumer/ingestor
mvn pkg
cd ../transformer
mvn pkg 
```

** Google Firebase**

Log into Google Cloud 

https://firebase.google.com/products/

Click **Get Started** 
Click **Add Project**
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

Node.js : Download from here https://nodejs.org/en/download/
```
npm install -g expo-cli
```


On your personal computer or where you will be simulating the mobile application 
```
git clone https://github.com/mpojeda84/connected-car-mapr-to-firebase.git
cd lib/
vi Firebase.js > paste in credentials 
```

We are now ready to start up our mobile application 
```
cd firebase 
mvn package 
npm install
expo start 
```

Keep hitting enter to get through the defaults 
When the Tunnel is ready hit “i” for iOS, this will launch the simulator 


