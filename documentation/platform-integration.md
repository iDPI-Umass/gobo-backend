
# Platform Integration
## Client Module Motivation

Gobo is motivated by iDPI's loyal client thesis. It articulates the idea that ordinary people need automated agents that center their needs. This is a rebalancing of the power dynamics encoded into social network architectures. Where provider platforms (Mastodon, Reddit, Bluesky, etc) don't contemplate each other in their interfaces, Gobo prioritizes the person who has their own needs.

That motivates Gobo's abstract notion of a post and its source (usually the author). Gobo tries to generalize and to hold platforms together, despite their incongruent interfaces. Gobo needs to employ sophisticated metaprogramming to achieve its goals as a loyal client. But we need reliable building blocks for such an architecture to be stable. So, the shape is important. They form a category that facilitates generalized feed construction operations.

As Gobo processes resources from provider platforms, we want to map them into Gobo's abstract representations. Gobo uses _client modules_ to do this. There's one for each provider platform, available in the `/client` [directory](https://github.com/iDPI-Umass/gobo-backend/tree/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients). 

Each one is a vital bridge. They take on the sprawling complexity of communicating with a given platform and wrangle it. They do whatever messy or complicated calculations are necessary, and they hide all of that from Gobo's internal interfaces. Freed of that noise and complexity, Gobo's internal interfaces can focus on abstract operations in peace. 

And importantly, this design is extensible. When we want to build a new bridge, we create a new client module and slot it into the worker. The rest of this document talks about what functions a client module needs to support.

## Client Module Requirements

In each of the modules within `/client`, there is a class that handles several different functions. These classes seat the persona we're proxying. For this explainer, I'll focus on the Mastodon class because its mapping is relatively straightforward.

Let's go through a manifest of what this class implements:

### Platform Name
You'll need to decide on the name for this host provider within the Gobo system. This `platform` field will appear on resources associated with it. You'll also need to add this name to the [list of supported platforms](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/api_specification/spec.yaml#L29-L33) so the Gobo HTTP API recognizes the platform during bootstrapping.

### Identity Seat
We pass a Gobo `identity` into the [constructor](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L131-L134) and the instance keeps it handy when we make requests to the provider platform. 

### Identity Bootstrapping Methods
In the Gobo frontend client, we ask a person for permission to access the provider platform on their behalf. The authentication and authorization mechanisms vary among provider platforms, which complicates this. 

We do a handshake with the Gobo HTTP API, starting with the [onboard identity start flow](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/handlers/action.py#L9). This handler responds with a URL the client should navigate to start the authorization process. For platforms that use OAuth, this navigates to their OAuth consent page with Gobo client details. Notice that we use a [helper module that wraps the main client module](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/platform_models/mastodon.py#L9). The helper orchestrates Gobo database resources, but it depends on the client class to provide the actual URL.

Once the Gobo frontend client has secured consent to access the provider platform, it confirms the complete registration with the Gobo backend. The frontend makes a request to the [onboard identity callback flow](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/handlers/action.py#L30). Again, we're invoking a helper module to validate the callback and [confirm the identity](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/platform_models/mastodon.py#L50), but we need the underlying client class to give us required information.

Implementers of new client modules would need to implement both the [underlying class methods](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L137-L182) and the above helpers that support the HTTP request flow. However, the specifics will depend on the platform's quirks.

### List Sources and Map Sources
Every client class will need a method called `list_sources` that [pulls a persona's follow list for a given platform](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L334) . We want the full list, which might include the persona themselves. This method will also need to take rate limiting and pagination into consideration. This method should return a dictionary that includes a list of source primitives.

This hooks into the Gobo worker's general worker flow that will [pull sources, then map them into Gobo resources](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/jobs/tasks/flow.py#L22-L30). So the output of this method is passed to another required class method, _map_sources_. This method accepts the dictionary from _list_sources_ and returns a list of Gobo sources.

Note the lack of control flow in [assembling the final source](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L244-L253). Gobo has had prior success with a multi-step mapping to help wrangle control flow complexity here. In the Mastodon example, we use [a special mapping class](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L356) to "preprocess" the data structure ahead of its final mapping. The nature of the final mapping depends on the platform and author preferences, but I recommend a similar strategy. 

### Get Post Graph and Map Posts
Every client class will need a method called _get_post_graph_ that [pulls a list of posts from a source and resources from the surrounding social graph](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L369) 

Let's break apart the Mastodon example:

First, [we grab the primary posts](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L370-L431) We paginate through the author's feed, looking for posts that we don't already have and being mindful of rate limiting. These posts will be fully upserted after we map them. Again, we take a pre-processing approach, with [a class that smoothes out some of the control flow messiness of this operation](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L33)

Next, we're ready to look for surrounding resources. These "partial" posts are important to track, but I've kept them apart because we're not guaranteed to have their full representation. They're not the focal point of this effort.

We go through the feed looking for [unique posts that have been shared](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L434-L442)

Then we do the same for [post replies](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L444-L462). Because Mastodon supports it, we actually ask for a thread of all post ancestors so we can assemble a more comprehensive post graph. We keep track of unique posts for the final mapping.

Once we have a full listing of unique posts, we [gather a list of unique post authors](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L466-L476) Not all of these will be sources that anyone within Gobo follows, but we want to map them all the same.

Then we can [return a listing](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L480-L484) of primary and secondary posts, along with authors

Just as with `list_sources`, this `get_post_graph` method hooks into the [worker flow](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/jobs/tasks/flow.py#L54-L58) The result gets passed to the _map_posts_ method that this class will need to implement.

In the [Mastodon example](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L258), the post pre-processing allows us to avoid excessive control flow and focus on mapping fields. We also [produce primitives of post edges](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L297-L324). The worker will resolve these platform IDs into Gobo IDs and store the final edges automatically. So authors can focus on returning a final dictionary containing a list of final primary posts, a list of secondary posts, and a list of edge primitives.
### Create Post
Authors might want to support crossposting from the Gobo frontend to the target platform. There is a [Gobo HTTP handler](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/handlers/person_post.py#L8) that accepts this request. It's generalized to accept a batch request. It then [dispatches tasks out to each platform worker](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/handlers/person_post.py#L55-L64)

However, you'll need to write a [second round of dispatching](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/jobs/mastodon.py#L69) to handle images management before ultimately dispatching to the client class.

The client class will also need to implement a _create_post_ method. In the [Mastodon example](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/clients/mastodon.py#L184) we needed to handle some conditional logic around reposts and replies and media handling. Overall, we marshal post data from the abstract representation and map it back into a form acceptable to Mastodon.
### Proxied Social Actions
Authors might want to support proxied social actions for posts represented in Gobo. These are actions such as "like", "repost", etc. These are modeled as edges between a given persona and the target post.

Again, there are generalized HTTP handlers to [PUT](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/handlers/person_post_edge.py#L35) and [DELETE](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/api/handlers/person_post_edge.py#L71) these edges. The handlers then dispatch a task to the platform worker that ultimately invokes platform client methods. However, you'll need to write a [second round of dispatching](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/jobs/mastodon.py#L93) from the abstract actions into the platform-specific actions.


### Worker Infrastructure
Authors will need to add a thread that runs worker tasks associated with a new platform. Ultimately, this [thread gets started](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/main.py#L54) in the worker's main. But you'll also need to:

- [Define a queue using the platform name](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/queues.py#L134)
- [Define a thread using the above queue](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/threads.py#L99-L102)  (use `Thread`. You can ignore `MiniThread`)
- [Define a case in the inter-process message queue](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/jobs/api.py#L53) This allows dispatches from the API container
- [Define the second tier task dispatching that's specific to this platform](https://github.com/iDPI-Umass/gobo-backend/blob/5fcbe37b17ea8630ed4aae045d186a20c9cf3184/worker/jobs/smalltown.py) This is what listens for create post and proxied actions. I'm using Smalltown here because it's more clear.