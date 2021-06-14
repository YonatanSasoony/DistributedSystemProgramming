# DistributedSystemProgramming

## Submitted by Yonatan Sasoony 205916265 and Yossy Carmeli 204752406

## How To Run Our Project?
java -jar LocalApplication.jar inputFileName1... inputFileNameN outputFileName1... outputFileNameN n [terminate]
- inputFileNameI is the name of the input file I.
- outputFileName is the name of the output file.
- n is the workers’ files ratio (reviews per worker).
- terminate indicates that the application should terminate the manager at the end (optional).

## How The Program Works?

### Local Application
The application resides on a local (non-cloud) machine. Once started, it reads the input file from the user, and:
- Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node.
- Uploads the file to S3.
- Sends a message to an SQS queue, stating the location of the file on S3
- Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.
- Downloads the summary file from S3, and create an html file representing the results.
- In case of terminate mode (as defined by the command-line argument), sends a termination message to the Manager.

### The Manager
The manager process resides on an EC2 node. It checks a special SQS queue for messages from local applications. Once it receives a message it:
- **In the case of new task message**:
- Download the input file from S3.
- Distribute the operations to be performed on the reviews to the workers using SQS queue/s.
- Check the SQS message count and starts Worker processes (nodes) accordingly.
    - The manager should create a worker for every n messages (as defined by the command-line argument), if there are no running workers.
    - If there are k active workers, and the new job requires m workers, then the manager should create m-k new workers, if possible.
    - Note that while the manager creates a node for every n messages, it does not delegate messages to specific nodes. All of the worker nodes take their 
	messages from the same SQS queue; so it might be the case that with 2n messages, hence two worker nodes, one node processed n+(n/2) messages, 
	while the other processed only n/2.

- **In case the manger receives response messages from the workers (regarding input file), it:**
- Creates a summary output file accordingly,
- Uploads the output file to S3,
- Sends a message to the application with the location of the file.
- **In case of a termination message, the manager:**
- Should not accept any more input files from local applications. However, it does serve the local application that sent the termination message.
- Waits for all the workers to finish their job, and then terminates them.
- Creates response messages for the jobs, if needed.
- Terminates.

### The Workers
A worker process resides on an EC2 node. His life cycle:
Repeatedly:
- Get a message from an SQS queue.
- Perform the requested job, and return the result.
- Remove the processed message from the SQS queue.

### The Flow
1. The LocalApplication uploads the input files to S3 and sends a SQS message to the Manager indicating the information for downloading the uploaded files.
2. The Manager downloads the files from S3, and distributes sentiment analysis and entity extraction tasks to the Workers using SQS.
3. The Worker performs the sentiment analysis and entity extraction tasks, and sends the output back to the Manager using SQS.
4. The Manager collects all the outputs from the Workers and create a summary for each input file, uploads the content of the summary to S3 and sends a SQS message 
back to the LocalApplication indicating the information for downloading the uploaded content.
5. The LocalApplication downloads the summary from S3, and creates an HTML file. If the LocalApplication got terminate as an argument it sends terminate message to the
 Manager.

### More Detailed Flow
1. Local Application uploads the file with the list of reviews urls to S3.
2. Local Application sends a message (queue) stating the location of the input file on S3.
3. Local Application checks if a manager is active and if not, starts it.
4. Manager downloads a list of reviews.
5. Manager distributes sentiment analysis and entity extraction jobs on the workers.
6. Manager bootstraps nodes to process messages.
7. Worker gets a message from a SQS queue.
8. Worker performs the requested job/s on the review.
9. Worker puts a message in a SQS queue indicating the original reviewtogether with the output of the operation performed (sentiment/extracted entities).
10. Manager reads all Workers' messages from SQS and creates one summary file.
11. Manager uploads the summary file to S3.
12. Manager posts a SQS message about the summary file.
13. Local Application reads SQS message.
14. Local Application downloads the summary file from S3.
15. Local Application creates html output file.
16. Local application send a terminate message to the manager if it received <i>terminate</i> as one of its arguments.


### What type of instance did we used?
- ami-0a92c388d914cf40c
- types: T2.MICRO for the Manager and T2.MEDIUM for the Workers.

### How much time it took your program to finish working on the input files, and what was the n you used?
- About 10 minuts, 5 input files and n=49

▪ Did you think for more than 2 minutes about security? Do not send your credentials in plain text!
- Yes. We didn't send our credentials at all. We used roles and security groups in order to pass the AWS permissions to the EC2 instances.

▪ Did you think about scalability? Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion? Scalability is very important aspect of the system, be sure it is scalable!
- Yes. In order to support large amount of clients we've created a SQS queue for every local application and input file combination.

▪ What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!
- The worker deletes his message only after successfully complete analyzing and sends his response for the message. In a case a worker dies/fails, the message wont be deleted and after his message's visibility time will ran out, another worker will receive it.
   Besides that, the manager checks if there are any meesing workers, and if so, initiates the necessary amount.

▪ Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!
- Using threads is a good idea in parts where our program has to perform many independent tasks. On the other hand, when we perform busy-wait for massages we did not use threads.
We used threads in our application twice:
1. Manager- when a manager recieves a message to perform, it initiates a thread that in charge of distributing the tasks for the workers, waits for all the task to be completed and then it creates, uploads the summary file and sends the location to the SQS queue for the local application.
2. LocalApplication - when the local application receives the location of the summary file from the manager, it initiates a thread to download the summary file, process it and creates the HTML file.

▪ Did you run more than one client at the same time? Be sure they work properly, and finish properly, and your results are correct.
- Yes. We ran 3 local applications at the same time, each with different inputs. All the outputs created properly and everything has been done simultaneously.

▪ Do you understand how the system works? Do a full run using pen and paper, draw the different parts and the communication that happens between them.
- Yes. We understand the flow and how to system works. As described above.

▪ Did you manage the termination process? Be sure all is closed once requested!
- Yes. once the manager receives the termination message, it won't deal with new tasks. it waits for all the other tasks to be completed. Once everything has completed (i.e summary files has been uploaded and sent), it shuts down the workers and itself.

▪ Are all your workers working hard? Or some are slacking? Why?
- Each worker runs infinitly and waits for messages and handles them. 
Because we are not delegating specific users per task, there can be a situation that some workers will do more tasks than others.

▪ Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!
- The manger, as well as the other applications, does only what he's supposed to do, as described above.

▪ Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?
- Yes, a distributed system is a system with multiple components located on different machines that communicate and coordinate actions in order to appear as a single system to the end-user.
Yes, The manager waits for the local application to upload files to S3. The manager waits for the workers to analize the data. And the local application waits for the manager until it will make the summary file.
