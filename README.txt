The tweet analyzer

Asaf Sarid 301465258
Ohad Cohen 200654606


###Security###
In order to keep our credentials safe, we added them into the script in run-time (not hard coded). We create file of credentials on the local memory of the manager and the workers, and when we run their program, they read it and use it for the AWS clients. 


###Scalability###
Our manager designed to handle many clients. The limit is depend on the size of the disk of the machine that the manager is running on. 
When receiving new task from local application, we get the input file, create output file on the disk with the input file number of lines, and send each line to the workers (to analyze). The big advantage of this design is that the number of tasks don’t increase the memory of the program, hence the size of our program memory is stable (almost constant). The disadvantage is that when receiving message from worker, we need to read the counter of remaining lines for this file, remove it and update it (in addition to writing the output data), this increasing the time of this action.
The trade-of in this situation is longer time to handle requests, but almost unlimited number of tasks can be handled (assuming unlimited disk size).


###Persistance###
We tried to create our program as persistent as possible. If the manager terminated for some reason, there is almost nothing we can do (the local application will wait until time out for response). But we tried to avoid manager failure by wrapping almost all possible exceptions, and by simulating many scenarios of malfunction system behaviour.
As for the workers: The worker reads message and deletes it only after it sends the response (analyzed tweet) to the manager, if the worker dies in the middle of the analyzing, after visibly timeout (30 seconds default), the message will be visible again for all other workers. The manager check the number of workers (using query to the AWS) every time it receives new task, and upon termination (to know the updated number of workers)


###Threads###
We didn't used thread in our workers for two reasons: 
there are many workers (it is up to the user decision), the user will determine the speed of the execution. 
The workers will probably run on small computers with one core, hence there is no reason to create more threads, it will only cause trashing (spend time on useless context switch).
In the manager we did used 2 separate threads. One thread is responsible for the queue from the local applications. It reads the input queue, and if there is task request, it creates output file, sends all  lines in the input file to the workers and create workers (their number is set according to your instructions). When receiving termination it update global flag and waits for the second thread to join.
The other thread read the input messages from the workers, and add it to the file of this task.
When both join, the main thread terminates the manager. this design allows us to be ready for income tasks, and to handle all the analyzed data concurrently. We could create thread per task but we dont think that it is more efficient.

 
###How to use###
create credential file in the directory of the project, call it ‘dspsass1.properties'
create bucket called ‘dsps1jarsbucketasafohad’, upload ‘manager.jar’ and ‘worker.jar’ to this bucket.
run TweetAnalyzer.jar with arguments: inputFileName outputFileName n terminate (optional)


###AMI###
workers- ami-f5f41398 (T2.small)
manager- ami-f5f41398 (T2.medium)


###Time to run the input file###
With 10 workers: 4:40 minutes (first task - including starting manager and workers + termination)
With 10 workers: 41 seconds (non-first task)


###Design of our project###
Local application- 
the local application first parse the input arguments, then uploads the input file. It checks if the manager instance is running (if not, it runs the manager). Then send message to the manager.
After sending the message, the local application reads the queue from the manager and seek for message with its id, if found- downloading the output file and create html file from it.
If the user requested manager terminating, the local application sends termination message to the manager and waits for its response, upon receiving it deletes the relevant queues.

Worker-
The worker connect to the queues from and to the manager, start reading messages. If it is termination message from the manager, the worker response with the needed statistics.
If this is regular message, the worker isolate the tweet, analyze it and send the response to the manager with the task id (so manager will know to which file it should be written).

Manager- 
The manager starts with two threads, one is responsible for reading and handling tasks from local applications (and terminate message), and the other is responsible for reading and adding to output file the messages from the workers (it will continue running until termination message has arrived and there are no open tasks). 
