# beam-concurreny-examples

Apache Beam is a powerful and flexible stream and batch processing model that allows you to develop data processing pipelines that are portable across various execution engines. When it comes to scaling your Apache Beam pipeline, one key aspect to consider is leveraging multithreading/Asynchronous Programming for improved performance.

## Scaling in Apache Beam with Multi-Threading and Asynchronous Programming

Multi-threading involves the concurrent execution of multiple threads, enabling the processing of multiple elements simultaneously. In Apache Beam, this can be achieved by leveraging the parallel processing capabilities of your pipeline.

Asynchronous Programming allows non-blocking execution of tasks, enabling the system to continue processing other tasks while waiting for certain operations to complete. This is particularly useful in scenarios where I/O operations or network calls might introduce latency.

## This repo contains three examples
  ### 1. Api.py:  
  > The first example is a basic implementation making API calls through python. This is a pretty slow mechanism as each api request is made sequentially one after the other. For example, if an api call takes 0.3 sec on avg, 100 calls would take 30 sec. We could improve the performance by increasing the number of worker nodes using Dataflow runner.
  ### 2. Multithreaded_api.py: 
  > The second example walks through a multi-threaded implementation of api calls in python. Multi-threading is a common way to improve performance in scenarios where we need to make a bunch of API requests. In this approach, each thread would be concurrently making API calls which improves the performance drastically. This approach is more suitable in CPU intensive task.
  ### 3. async_api.py : 
> The third example is an asynchronous implementation of making api requests in python. This is the fastest way to make api calls in a very short time as an api call is an I/O bound task. Asynchronous approach is the most efficient way to handle I/O bound tasks

The individual examples will have more detailed explainations


To incorporate multi-threading into Apache Beam, it's essential to have a grasp of concepts like ParDo transforms and DoFn classes, along with a deep understanding of the methods provided by a DoFn class. These components play a crucial role in defining and executing parallel processing within your Apache Beam pipeline.


