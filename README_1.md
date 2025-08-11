# Spark Error Task Test

This project demonstrates **common issues that Spark jobs may encounter** along with their log outputs, for assisting in Sonic information analysis.  
It includes:  
✅ Original `reduceByKey` job (causing data skew)  
✅ Optimized version (random prefix + two-stage aggregation)  
✅ Spark SQL version (DataFrame implementation)  
✅ Various Java exceptions combined with Spark job failures

## 🚀 How to Run
The project is deployed on VMware with three virtual machines, where Hadoop and Spark are installed. Jobs are scheduled by YARN and run in **cluster mode**.

### 1️⃣ Build the Project
```bash
mvn clean package
```


### 2️⃣ Submit to Spark
```bash
spark-submit   --class com.example.spark.*   target/spark-data-skew-demo-1.0-SNAPSHOT.jar
```

### 3️⃣ View Spark UI
- While running: [http://localhost:8088](http://localhost:8088)
- After completion: [http://localhost:18080](http://localhost:18080)  
  Here you can see **detailed information for each application** and **event logs for each application**.

---

## 🔥 Detailed Cases 

- `DivideByZeroJob.java` → Test whether subsequent jobs execute when a divide-by-zero error occurs in the third job of four total jobs.
- **Final Status**: **Failed**  
![](images/figure1.png)
- **Execution Detail**: Failed during the third job (Job ID: 2 — job IDs start from 0).  
![](images/figure2.png)
- **Logs**: `SparkListenerJobEnd` events are recorded up to Job ID 2. The following job did not execute.
![](images/figure3.png)
---
- `DivideByZeroJob2.java` → Test behavior when a divide-by-zero error occurs in the fourth job of four total jobs.
- **Final Status**: **Failed**  
  ![](images/figure4.png)
- **Execution Detail**: Failed during the fourth job (Job ID: 3).  
  ![](images/figure5.png)
- **Logs**: Recorded up to Job ID 3; the application failed after this point.  
  ![](images/figure6.png)
---
- `DivideByZeroJob3.java` → Insert a Java divide-by-zero exception between the first and second job.
- **Final Status**: **Failed**  
  ![](images/figure7.png)
- **Execution Detail**: Threw `java.lang.ArithmeticException`.  
  ![](images/figure8.png)
- **Logs**: Only the first `SparkListenerJobEnd` event was recorded; it was successful. However, no subsequent jobs executed, and no related logs were recorded — likely because the error was thrown on the Driver side before scheduling the next job.
  ![](images/figure9.png)
---
- `FileNotFoundJob.java` → Trigger a Java file-not-found exception after a completed job.
- **Final Status**: **Failed**  
  ![](images/figure10.png)
- **Execution Detail**: Threw `java.io.FileNotFoundException`.  
  ![](images/figure11.png)
- **Logs**: Only the first job’s `SparkListenerJobEnd` was recorded as successful. The application still failed, and the event log did not record the Java exception.  
  ![](images/figure12.png)
---
- `FileNotFoundJob2.java` → Have the second job attempt to read a non-existent file using `textFile`.
- **Final Status**: **Failed**  
  ![](images/figure13.png)
- **Execution Detail**: Hadoop threw `org.apache.hadoop.mapred.InvalidInputException`.  
  ![](images/figure14.png)
- **Logs**: Only the first job’s `SparkListenerJobEnd` was recorded; the failing job did not execute because Spark detected the missing file during job submission.  
  ![](images/figure15.png)
---
- `FileNotFoundJob3.java` → Insert a Java exception between two jobs, and make the second job attempt to read a non-existent file.
- **Final Status**: **Failed**
  ![](images/figure16.png)
- **Execution Detail**: Threw `java.io.FileNotFoundException`.  
  ![](images/figure17.png)
- **Logs**: Only the first job’s `SparkListenerJobEnd` was recorded as successful. The application still failed, and the event log did not record the Java exception.  
  ![](images/figure18.png)
---
- `NullPointerJob.java` → Trigger a null pointer exception in the second job.
- **Final Status**: **Failed**  
  ![](images/figure19.png)
- **Execution Detail**: Spark threw `org.apache.spark.SparkException`.    
  ![](images/figure20.png)
- **Logs**: The final `SparkListenerJobEnd` was recorded, containing detailed exception information.  
  ![](images/figure21.png)
---
- `MemoryExplodeExample.java` → Allocate more memory than configured for the job.
- **Final Status**: **Failed**
  ![](images/figure22.png)
- **Execution Detail**: Hit maximum executor failure count.  
  ![](images/figure23.png)
- **Logs**: Three executors died, each with a `SparkListenerExecutorRemoved` event showing exit code 143, indicating that YARN terminated the executors due to external signals (likely OOM).  
  ![](images/figure24.png)
  ![](images/figure25.png)
---
- `DynamicExecutorExample.java` → Test dynamic executor allocation and observe normal executor termination.
- **Final Status**: **Succeeded**  
  ![](images/figure26.png)
- **Execution Detail**: Five executors were terminated normally.  
  ![](images/figure27.png)
- **Logs**: All corresponding `SparkListenerExecutorRemoved` events showed normal removal without errors.  
  ![](images/figure28.png)
  ![](images/figure29.png)
