from twitter_handler import TwitterHandler
from reddit_handler import RedditHandler
from mastodon_handler import MastodonHandler

import psycopg
from psycopg.rows import class_row
from psycopg_pool import AsyncConnectionPool, AsyncNullConnectionPool

import asyncio
import gobo_types

from dotenv import load_dotenv

from db_utils import insert_post_batch
import datetime

import pdb
import sys

conn_str = "dbname={database} user={user} password={password} host={host}".format(
            user='gobo_backend', password='idpi-gobo-twooh', host='localhost',
            database='gobo_dev')

load_dotenv()

class ReadDaemon:
  def __init__(self):
    # TODO(slane): Move these parameters to a .env file or other config file
    # Initialization of the DB connection pool needs to be done elsewhere
    self.pool = None

    self._max_age = 60 * 60 * 24 * 2 # 2 Days for now
    
    # Initialize Handlers
    self._twitter_handler = TwitterHandler()
    self._reddit_handler = RedditHandler()
    self._mastodon_handler = MastodonHandler()
    
    # Initialize Queues
    self._identity_queue = None
    self._source_queue = None
    self._post_queue = None
    
    self._num_identity_consumers = 1
    self._identity_delay = 1
    self._pull_identity_frequency = 60 * 60 * 12 # 12 Hours for now
    
    self._num_source_consumers = 4
    self._source_delay = 1
    self._pull_source_frequency = 60 * 15 # 15 minutes

    self._num_post_consumers = 10
    
    self._post_batch_size = 10
    self._post_wait_time = 10 # If there are no posts to insert, wait N seconds before checking again
    self._post_wait_count = 3 # If there are posts and waiting has happened N times, just insert

  def __select_handler(self, obj):
    if obj.base_url == "twitter.com":
      return self._twitter_handler
    elif obj.base_url == "www.reddit.com":
      return self._reddit_handler
    else:
      return self._mastodon_handler

  #===========================================================
  #                   Identity Handling
  #===========================================================    
  async def __fill_identity_queue(self):
    async with self.pool.connection() as aconn:
      async with aconn.cursor(row_factory = class_row(gobo_types.Identity)) as acur:
        await acur.execute("SELECT * from identities")
        #await acur.execute("SELECT * from identities where base_url = %s", ["twitter.com"])

        async for identity in acur:
          if identity.last_updated:
            priority = identity.last_updated.timestamp()
          else:
            priority = 0
          self._identity_queue.put_nowait((priority, identity))
        print("Filled Identity Queue with {} items".format(self._identity_queue.qsize()))

  async def __identity_consumer(self):
    while True:
      try:
        timestamp, identity = await self._identity_queue.get()
        time_delta = self._pull_identity_frequency - (datetime.datetime.now().timestamp() - timestamp)
        
        if time_delta > 0:
          print("Sleeping for {} seconds, then retrieving {} Identity {}".format(time_delta, identity.base_url, identity.username))
          await asyncio.sleep(time_delta)
        handler = self.__select_handler(identity)
        print("Retreiving {} Identity: {}".format(identity.base_url, identity.username))

        await handler.update_identity_info(self.pool, identity)
        self._identity_queue.task_done()
        await asyncio.sleep(self._identity_delay)
      except (asyncio.CancelledError, KeyboardInterrupt):
        break
      except Exception as e:
        raise e

          
  async def __identity_manager(self):
    identity_consumers = [asyncio.create_task(self.__identity_consumer()) for i in range(self._num_identity_consumers)]
    print("Created {} Identity Consumers".format(len(identity_consumers)))
    while True:
      try:
        await self._identity_queue.join()
        await self.__fill_identity_queue()
      except (KeyboardInterrupt, asyncio.CancelledError):
        break
      except:
        raise
    for consumer in identity_consumers:
      consumer.cancel()
    await self._identity_queue.join()
    await asyncio.gather(*identity_consumers)

  #===========================================================
  #                   Source Handling
  #===========================================================    
  async def __fill_source_queue(self):
    #async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
    async with self.pool.connection() as aconn:
      async with aconn.cursor(row_factory = class_row(gobo_types.FollowedSource)) as acur:
        await acur.execute("SELECT * from followed_sources WHERE active")
        #await acur.execute("SELECT * from followed_sources where base_url = %s", ["www.reddit.com"])

        async for followed_source in acur:
          if followed_source.last_retrieved:
            priority = followed_source.last_retrieved.timestamp()
          else:
            priority = 0

          self._source_queue.put_nowait((priority, followed_source))
          
        print("Filled Source Queue with {} items".format(self._source_queue.qsize()))
  
  async def __source_consumer(self):      
    while True:
      try:
        timestamp, source = await self._source_queue.get()
        time_delta = self._pull_source_frequency - (datetime.datetime.now().timestamp() - timestamp)
        
        if time_delta > 0:
          print("Sleeping for {} seconds, then retrieving {} Source {}".format(time_delta, source.base_url, source.display_name))
          await asyncio.sleep(time_delta)

        handler = self.__select_handler(source)
        print("Retreiving {} Source: {}".format(source.base_url, source.display_name))

        await handler.process_source(self.pool, source, self._post_queue)
        self._source_queue.task_done()
        await asyncio.sleep(self._source_delay)
      except (asyncio.CancelledError, KeyboardInterrupt):
        break
      except:
        raise

          
  async def __source_manager(self):
    source_consumers = [asyncio.create_task(self.__source_consumer()) for i in range(self._num_source_consumers)]
    print("Created {} Source Consumers".format(len(source_consumers)))
    while True:
      try:
        await self._source_queue.join()
        print("Emptied source queue. Refilling")
        await self.__fill_source_queue()
      except (KeyboardInterrupt, asyncio.CancelledError):
        break
      except:
        raise
    for consumer in source_consumers:
      consumer.cancel()
    await asyncio.gather(*source_consumers)


  #===========================================================
  #                   Post Handling
  #===========================================================    
  async def __post_consumer(self):
    batch = []
    sleep_count = 0
    while True:
      try:
        post = None
        while not post:
          try:
            post = self._post_queue.get_nowait()
          except asyncio.QueueEmpty:
            post = None
            sleep_count += 1
            if sleep_count > self._post_wait_count:
              print("Waited long enough, try to insert posts")
              sleep_count = 0
              break
            print("No post found, sleeping for {} seconds".format(self._post_wait_time))
            await asyncio.sleep(self._post_wait_time)
          except:
            raise

        if post:
          batch = batch + [post]
          self._post_queue.task_done()
          print("Batch grew. New size: {}".format(len(batch)))

        if len(batch) >= self._post_batch_size or (batch and not post):
          print("Inserting {} posts into db".format(len(batch)))
          await insert_post_batch(self.pool, batch)
          batch = []
          sleep_count = 0

      except (asyncio.CancelledError, KeyboardInterrupt):
        break
      except:
        raise
       
  async def __post_manager(self):
    post_consumers = [asyncio.create_task(self.__post_consumer()) for i in range(self._num_post_consumers)]
    print("Created {} Post Consumers".format(len(post_consumers)))
    while True:
      try:
        await asyncio.sleep(10)
      except (KeyboardInterrupt, asyncio.CancelledError):
        break
      except:
        raise
    for consumer in post_consumers:
      consumer.cancel()

    await asyncio.gather(*post_consumers)
      
  #===========================================================
  #                   General Execution
  #===========================================================    
          
  async def run(self):
    # Initialize Pool
    self.pool = AsyncNullConnectionPool(conn_str)

    # Fill Queues
    self._identity_queue = asyncio.PriorityQueue()
    self._source_queue = asyncio.PriorityQueue()
    self._post_queue = asyncio.Queue()
    await self.__fill_identity_queue()
    await self.__fill_source_queue()
    
    # Start Managers
    identity_manager = asyncio.create_task(self.__identity_manager())
    source_manager = asyncio.create_task(self.__source_manager())
    post_manager = asyncio.create_task(self.__post_manager())

    
    await identity_manager
    await source_manager
    await post_manager
    
if __name__ == "__main__":
  daemon = ReadDaemon()
  asyncio.run(daemon.run())
