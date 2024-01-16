# Explanation

while a multi-threaded approach is significantly faster, it is not the most efficient way to make API calls. 
As an API call is an I/O bound task, using threads is not the best approach. This is because even at a thread 
level, each thread is going to be blocked until it gets a response from the api for each api call.

In an asynchronous approach, tasks can be started and continue execution without waiting for the previous task to complete. 
It allows the program to perform other operations while waiting for non-blocking tasks, such as API calls, to finish.
Therefore asynchronously making the API calls would exponentially improve the performance over the multi-threaded approach.

To implement this, we need to use the **aiohttp** library as the request library is synchronous by nature

```

    def _flush(self):

        results=[]

        async def fetch(session,element):
           url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(element['address'])
           url = url + "&key={}".format(self.api_key)
           start = time.time()

           async with session.get(url) as response:
               res=await response.json()
               time_taken = time.time() - start
               element['time_taken'] = time_taken

           while(res['status'] =='OVER_QUERY_LIMIT'):
               async with session.get(url) as response:
                   res = await response.json()
                   time_taken = time.time() - start
                   element['time_taken'] = time_taken

           element['geocode_response'] = str(res)
           return element

        async def by_aiohttp_concurrency(elements):
           # use aiohttp

            async with aiohttp.ClientSession() as session:
               tasks = []
               for element in elements:
                   tasks.append(asyncio.create_task(fetch(session,element)))

               original_result = await asyncio.gather(*tasks)
               for res in original_result:
                    results.append(res)

        asyncio.run(by_aiohttp_concurrency(self.buffer))
        # asyncio.run(by_aiohttp_concurrency(errors))
        for i in results:
           yield i

```
This approach is very similar to the multi-threaded approach. The DoFn classes in both the approaches look very similar.
The only difference the _flush method where the accumulated buffer is processed asynchronously as opposed to the the multi-threaded fashion.

To understand the code thoroughly, I would highly recommened learning the basics of asynchronous programming in python and usage of **aiohttp**
library.
