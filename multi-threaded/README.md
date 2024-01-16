# Explanation

Before diving into multi-threading in beam let's understand how multi-threading works in python

```
        with ThreadPoolExecutor(max_workers=32) as executor:
            future_to_index = {executor.submit(_request, i): i for i in self.buffer}
            for future in as_completed(future_to_index):
                yield future.result()

```

The above code shows a basic example of a mulit-threading in python. Key point to note is that, it takes a list of elements (self.buffer in the above code) as input and disturbutes
the elements to be processed parallely among the allocated threads.

This is not as straight forward to implement in apache beam, since the transforms in beam apply once for each element. The elements are not available as a list in beam but rather as
a Pcollection. So the idea is to get the elements of a pcollection into a list to utilize multi-threading in python similarly as shown in the above example.

The following code shows how to create a list buffer using a DoFn in beam.

```
class textapi_call(beam.DoFn):
    def __init__(self, api_key):
        # self.buffer = []
        self.api_key = api_key
        # self.session = requests.session()

    def start_bundle(self):
        self.buffer = []


    def process(self, element):
        self.buffer.append(element)
        if len(self.buffer) ==3000:
            yield from self._flush()
            self.buffer = []


    def finish_bundle(self):
        if self.buffer:
            for element in self._flush():
                yield WindowedValue(element, MIN_TIMESTAMP,[GlobalWindow])

    def _flush(self):

        ses = requests.session()

        def _request(element):
            address = element[3] + ", " + element[4] + ", " + element[5] + ", " + element[6] + ", " + element[7]
            geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
            if self.api_key is not None:
                geocode_url = geocode_url + "&key={}".format(self.api_key)
            start = time.time()
            results = ses.get(geocode_url)
            res = results.json()
            status = res['status']
            while(status=='OVER_QUERY_LIMIT'):
                results = ses.get(geocode_url)
                res = results.json()
                status = res['status']
            time_taken = time.time() - start
            # res = results.json()
            return [element[0], address, str(res), time_taken]


        with ThreadPoolExecutor(max_workers=32) as executor:
            future_to_index = {executor.submit(_request, i): i for i in self.buffer}
            for future in as_completed(future_to_index):
                yield future.result()

```

As you can see, a buffer (list) is initialized in a start_bundle method. The process method is used to accumulate a certain number of elements and once it hits a certain count the 
_flush method is called where all the multi-threading s performed. The finish_bundle is used to process the elements that do not hit the certain count in the buffer.

