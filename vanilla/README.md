#Explaination

You can find two ways of making api calls in this approach. The following function is basic way of making api calls in beam using the request library in python.
```
def api_resp2(address, api_key):
    url = findplace_url(address, api_key=api_key)
    params = { "inputtype": "textquery", "fields":"name,place_id,geometry,type,icon,permanently_closed,business_status"}
    # params = {}
    payload = {}
    headers = {}
    res = requests.get(url, params=params)
    results = json.loads(res.content)
    return results

```
The above function can be simply called inside a map transform in apache beam as follows:

```
second_call = first_call | "call findplace" >> beam.Map(lambda x: call_findplace(x,api_key))

```
You can find this part of code not being used as I tried to use a single session to make all the api calls. Ideally a session is supposed to decrease the total time taken significantly since by using a session we avoid creating a connection between the api for each request.

The following snippet shows how to implement a session in apache beam:

```
class textapi_call(beam.DoFn):
    def __init__(self, api_key):
        self.api_key = api_key
        
    def setup(self):
        self.session = requests.session()

    def process(self, element):
        address = element[3] + ", " + element[4] + ", " + element[5] + ", " + element[6] + ", " + element[7]
        url = findplace_url(address, api_key=self.api_key)
        params = {"inputtype": "textquery",
                  "fields": "name,place_id,geometry,type,icon,permanently_closed,business_status"}
        start = time.time()
        res = self.session.get(url, params=params)
        results = json.loads(res.content)
        time_taken = time.time() - start

        return [[element[0], address, str(results), time_taken]]


    def teardown(self):
        self.session.close()

```

I am used setup method of a Dofn to establish a session and the teardown method to close the session. The process is being used to make the api calls using the session object created in the setup.

This would be called using a Pardo transform as follows:

```
first_call = lines_list | "ext add" >> beam.ParDo(textapi_call(api_key))
```

Though sessions are supposed to increase performance, this was not the case with the dataflow runner. Apparently, it would be difficult for dataflow to maintain a consistent session between various workers and the bundles in which it processes the elements.



