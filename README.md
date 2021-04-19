# DistributedSystemProgramming

## Submitted by Yonatan Sasoony 205916265 and Yossy Carmeli 204752406

## How To Run Your Project?
java -jar LocalApplication.jar inputFileName1... inputFileNameN outputFileName1... outputFileNameN n [terminate]
- inputFileNameI is the name of the input file I.
- outputFileName is the name of the output file.
- n is the workers’ files ratio (reviews per worker).
- terminate indicates that the application should terminate the manager at the end (optional).

## How The Program Works?

**Local Application**
The application resides on a local (non-cloud) machine. Once started, it reads the input file from the user, and:
- Checks if a Manager node is active on the EC2 cloud. If it is not, the application will start the manager node.
- Uploads the file to S3.
- Sends a message to an SQS queue, stating the location of the file on S3
- Checks an SQS queue for a message indicating the process is done and the response (the summary file) is available on S3.
- Downloads the summary file from S3, and create an html file representing the results.
- In case of terminate mode (as defined by the command-line argument), sends a termination message to the Manager.

**The Manager**
The manager process resides on an EC2 node. It checks a special SQS queue for messages from local applications. Once it receives a message it:
- **For the case of new task message**:
- Download the input file from S3.
- Distribute the operations to be performed on the reviews to the workers using SQS queue/s.
- Check the SQS message count and starts Worker processes (nodes) accordingly.
    - The manager should create a worker for every n messages (as defined by the command-line argument), if there are no running workers.
    - If there are k active workers, and the new job requires m workers, then the manager should create m-k new workers, if possible.
    - Note that while the manager creates a node for every n messages, it does not delegate messages to specific nodes. All of the worker nodes take their messages from the same SQS queue; so it might be the case that with 2n messages, hence two worker nodes, one node processed n+(n/2) messages, while the other processed only n/2.

- **In case the manger receives response messages from the workers (regarding input file), it:**
- Creates a summary output file accordingly,
- Uploads the output file to S3,
- Sends a message to the application with the location of the file.
- **In case of a termination message, the manager:**
- Should not accept any more input files from local applications. However, it does serve the local application that sent the termination message.
- Waits for all the workers to finish their job, and then terminates them.
- Creates response messages for the jobs, if needed.
- Terminates.

**The Workers**
A worker process resides on an EC2 node. His life cycle:
Repeatedly:
- Get a message from an SQS queue.
- Perform the requested job, and return the result.
- Remove the processed message from the SQS queue.

**The Flow**
1. The LocalApp sends the input files to the Manager using S3, and also sends a SQS message to the Manager for letting him know that input file were sent.
2. The Manager downloads the files from S3, and distributes sentiment analysis and entity extraction tasks to the Workers using SQS.
3. The Worker performs the sentiment analysis and entity extraction tasks, and sends the output back to the Manager using SQS.
4. The Manager collects all the outputs from the Workers and create a summary for each input file, and sends the summary back to the LocalApp using S3 and lets the LocalApp know about it using SQS.
5. The LocalApp downloads the summary from S3, and creates HTML file. If the LocalApp got terminate as an argument it sends terminate message to the Manager.

**More Detailed Flow**
1. Local Application uploads the file with the list of reviews urls to S3.
2. Local Application sends a message (queue) stating the location of the input file on S3.
3. Local Application does one of the following:
     - Starts the manager.
     - Checks if a manager is active and if not, starts it.
4. Manager downloads a list of reviews.
5. Manager distributes sentiment analysis and entity extraction jobs on the workers.
6. Manager bootstraps nodes to process messages.
7. Worker gets a message from an SQS queue.
8. Worker performs the requested job/s on the review.
9. Worker puts a message in an SQS queue indicating the original reviewtogether with the output of the operation performed (sentiment/extracted entities).
10. Manager reads all Workers' messages from SQS and creates one summary file.
11. Manager uploads the summary file to S3.
12. Manager posts an SQS message about the summary file.
13. Local Application reads SQS message.
14. Local Application downloads the summary file from S3.
15. Local Application creates html output file.
16. Local application send a terminate message to the manager if it received <i>terminate</i> as one of its arguments.

**What type of instance did we used?**
- ami-0a92c388d914cf40c
- types: T2.MICRO for the Manager and T2.MEDIUM for the Workers.

**How much time it took your program to finish working on the input files, and what was the n you used?**
- About 5 minuts, 2 input files and n=10