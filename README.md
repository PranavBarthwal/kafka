# 📍 Apache Kafka

Apache Kafka is a distributed event streaming platform used for building real-time data pipelines and streaming applications. It is designed to handle high-throughput, fault-tolerant, and scalable messaging. Kafka is widely used for log aggregation, real-time analytics, and event-driven architectures.

In simpler terms, Apache kafka is like a communication system that helps different parts of a computer system exchange data by publishing and subscribing to topics.



# 📍 Kafka's Publisher-Subscriber model
![image](https://github.com/user-attachments/assets/8d38bcea-0ee5-41fb-89dd-8b619e77827c)

In **Kafka's Publisher-Subscriber model** (Pub-Sub model), producers (publishers) send messages to **topics**, and consumers (subscribers) read messages from these topics. Kafka brokers store and distribute these messages efficiently.  

### **How It Works:**
1. **Producers publish** messages to a topic.  
2. **Kafka brokers store** these messages in **partitions** (subdivisions of topics for scalability).  
3. **Consumers subscribe** to topics and process messages.  
4. **Consumer groups ensure** each message is processed by only one consumer in the group.  



# 📍 **Zomato’s Kafka-based Architecture for Real-Time Delivery Tracking**  

### **Problem with Traditional Architecture**  
In a traditional architecture, Zomato would frequently retrieve and store the **delivery boy’s location** in the **database (DB)** and send updates to the **user**. Given Zomato’s scale, this would lead to:  
- **Excessive DB hits** every second.  
- **Performance issues** due to limited DB throughput.  
- **Risk of DB crashes** from high-frequency reads and writes. 

### **Diagram:**  
```
+------------------+       +------------------+       +------------------+
| Delivery Boy    | -----> |   Database (DB)  | <----- |      User       |
| (Sends Location)|        | (Frequent Writes)|        | (Requests Data) |
+------------------+       +------------------+       +------------------+
                            ▲    ▲    ▲    ▲
                            |    |    |    |
              High DB Load Due to Frequent Writes & Reads
``` 

### **Kafka-based Pub-Sub Model for Zomato**  
To handle high scale and volume efficiently, Zomato can implement a **Kafka-based Publish-Subscribe (Pub-Sub) model**, where:  
- **Producers (Delivery Boys)** publish location updates to **Kafka topics**.  
- **Kafka** efficiently handles high-throughput streaming.  
- **Consumers (Users)** subscribe to receive real-time updates.  
- The **server processes** data and stores it in the DB in bulk after order completion in a **batch process**.  

### **Diagram:**  
```
+------------------+       +------------------+       +------------------+
| Delivery Boy    | -----> |      Kafka       | -----> |      User       |
| (Sends Location)|        | (High Throughput)|        | (Receives Live  |
+------------------+        |  Pub-Sub Model) |        |  Updates)       |
                            +------------------+
                                    │
                                    ▼
                        +----------------------+
                        |   Database (DB)      |
                        | (Bulk Storage After  |
                        |  Order Completion)   |
                        +----------------------+
``` 

### **Why Do We Still Need a Database Along with Kafka?**  
Kafka is a **message broker**, not a **permanent storage solution**. While Kafka can retain messages for a configurable period, we still need a **database** for:  
- **Long-term storage** – Order and delivery history must be stored permanently.  
- **Querying and analytics** – Databases provide structured access to historical data.  
- **Data consistency** – Kafka handles streaming but does not ensure **ACID compliance** like relational databases.  
- **Data retrieval** – If a user wants to check past orders, this data must be stored in a DB, not Kafka.  

### **Comparison Between Traditional DB-Based Approach and Kafka-Based Approach**  

| Feature                     | **Traditional DB-Based Approach** | **Kafka-Based Approach** |
|-----------------------------|----------------------------------|-------------------------|
| **Data Flow**               | Delivery boy updates DB, user fetches from DB | Delivery boy publishes to Kafka, user subscribes to real-time updates |
| **Database Load**           | Very high due to frequent writes and reads | Minimal as updates are stored in Kafka and written to DB in batches |
| **Scalability**             | Limited due to DB bottlenecks | Highly scalable with Kafka's distributed architecture |
| **Real-Time Updates**       | No, updates depend on DB read frequency | Yes, users get instant updates through Kafka |
| **Latency**                 | High due to DB query and write delays | Low as Kafka streams data in real-time |
| **Reliability**             | Risk of DB crashes under high load | High as Kafka provides replication and fault tolerance |
| **Storage Efficiency**      | Inefficient, as every update is stored in the DB | Efficient, as only final delivery data is stored in the DB |
| **System Complexity**       | Simpler but not optimized for scale | Slightly more complex but highly optimized for performance |
| **Cost Efficiency**         | High cost due to heavy DB infrastructure | Lower cost as Kafka handles high throughput without DB dependency |
| **Use Case Suitability**    | Works for small-scale applications | Best for large-scale, high-throughput systems like Zomato |




