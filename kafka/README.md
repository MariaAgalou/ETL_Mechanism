# Commands to run and use Kafka (on Windows)

1. Navigate to the directory where Kafka is installed.
```
cd C:\Users\Path\To\Kafka\Directory
```

2. Start **Zookeeper** on a Windows Powershell window.
```
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

3. Start **Kafka** on a new Windows Powershell window.
```
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

4. **Create a topic** (e.g. named test) on a new Windows Powershell window.
```
.\bin\windows\kafka-topics.bat --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

5. List **existing topics**.
```
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

6. Start **Kafka Producer** for the topic you created previously, to send messages to this topic.
```
.\bin\windows\kafka-console-producer.bat --topic test --bootstrap-server localhost:9092
```

7. Start **Kafka Consumer** for the same topic, to receive messages from this topic.
```
.\bin\windows\kafka-console-consumer.bat --topic test --bootstrap-server localhost:9092 --from-beginning
```

8. Stop Kafka and Zookeeper by pressing Ctrl+C, when you're done.
