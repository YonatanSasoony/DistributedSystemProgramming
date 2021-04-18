README

Submitted by Yonatan Sasoony 205916265 and Yossy Carmeli 204752406

********************************************************************************************************************************************************************************************
A text file called README with instructions on how to run your project, and an explanation of how your program works. It will help you remember your implementation.
Your README must also contain what type of instance you used (ami + type:micro/small/large…),
how much time it took your program to finish working on the input files, and what was the n you used.
********************************************************************************************************************************************************************************************
How To Run Your Project?
java -jar LocalApplication.jar inputFileName1... inputFileNameN outputFileName1... outputFileNameN n [terminate]
• inputFileNameI is the name of the input file I.
• outputFileName is the name of the output file.
• n is the workers’ files ratio (reviews per worker).
• terminate indicates that the application should terminate the manager at the end (optional).

How The Program Works?
The LocalApp sends the input files to the Manager using S3, and also sends a SQS message to the Manager for letting him know that input file were sent.
The Manager downloads the files from S3, and distributes sentiment analysis and entity extraction tasks to the Workers using SQS.
The Worker performs the sentiment analysis and entity extraction tasks, and sends the output back to the Manager using SQS.
The Manager collects all the outputs from the Workers and create a summary for each input file, and sends the summary back to the LocalApp using S3 and lets the LocalApp know about it using SQS.
The LocalApp downloads the summary from S3, and creates HTML file. If the LocalApp got terminate as an argument it sends terminate message to the Manager.

What type of instance did we used?
ami-0a92c388d914cf40c
types: T2.MICRO for the Manager and T2.MEDIUM for the Workers.

How much time it took your program to finish working on the input files, and what was the n you used?
About 5 minuts, 2 iput files, n=10

********************************************************************************************************************************************************************************************
Be sure to submit a README file. Does it contain all the requested information? If you miss any part, you will lose points. Yes including your names and ids.
▪ Did you think for more than 2 minutes about security? Do not send your credentials in plain text!
Security: In order to take care about our credentials we used security group ... TODO

▪ Did you think about scalability? Will your program work properly when 1 million clients connected at the same time? How about 2 million? 1 billion? Scalability is very important aspect of the system, be sure it is scalable!

צוואר בקבוק? לשאול על סקלביליות?
בגלל שכל התרדים של הMANAGER מאזינים לאותו תור קשה להם לאתר את ההודעות הרלוונטיות למשימה שלהם
אולי כדאי ליצור עוד תורים בין הWORKERS ל MANAGER? עבור כל LOCAL APP? ואז מעבירים לWORKERS בREQUEST (תור בודד) לאן לשלוח את הודעת הRESPONSE
(אחד לכל LOCAL APP) 
האם מימוש סקליביליות תפקידו לשפר את זמן הריצה? (יותר תורים למשל?) או להקצות יותר משאבים (יותר MANAGER למשל?)


▪ What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures.
 What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!
Each Worker deletes his task only after he finished to handle it, so in a scenario in which a Workers dies, after the visibility-time finished another Worker will handle the task.
what about if the manager dies? TODO many things..


▪ Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!
Using threads is a good idea in parts where our program has to perform many independent tasks. On the other hand, when we performs busy-wait for massages we did not used threads.
 
▪ Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?
Yes, a distributed system is a system with multiple components located on different machines that communicate and coordinate actions in order to appear as a single system to the end-user.
Yes, The Manager waits for the LocalApp to upload files to S3. The Manager waits for the Workers to analize the data. And the LocalAPp waits for the Manager until he will make the summary file.

  


בקבלת הודעות- איך מקבךים הודעות בצורה יעילה? ללא BUSY WAIT?




איך עובדים עם קוד משותף בין פרויקטים?









