import psycopg
from psycopg.rows import class_row
from psycopg_pool import AsyncConnectionPool
from datetime import datetime
import gobo_types 
import pdb

conn_str = "dbname={database} user={user} password={password} host={host}".format(
            user='gobo_backend', password='idpi-gobo-twooh', host='localhost',
            database='gobo_dev')

def insert_pending_registration(user_id: str,
                                base_url: str,
                                oauth_token: str = None,
                                oauth_token_secret: str = None,
                                saved_state: str = None):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
  
      cursor.execute('''
        INSERT INTO pending_registrations (user_id,
                                           base_url,
                                           oauth_token,
                                           oauth_token_secret,
                                           saved_state,
                                           request_time)
        VALUES(%(user_id)s, %(base_url)s, %(token)s, %(secret)s, %(state)s, %(time)s)
        ON CONFLICT (user_id)
        DO UPDATE
        SET
          base_url = %(base_url)s,
          oauth_token = %(token)s,
          oauth_token_secret = %(secret)s,
          saved_state = %(state)s,
          request_time = %(time)s
        WHERE pending_registrations.user_id = %(user_id)s
      ''',
                     {'user_id': user_id,
                      'base_url': base_url,
                      'token': oauth_token,
                      'secret': oauth_token_secret,
                      'state': saved_state,
                      'time': datetime.now()})

def get_pending_registration(user_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor(row_factory = class_row(gobo_types.PendingRegistration)) as cursor:
  
      cursor.execute('''
        SELECT * from pending_registrations
        WHERE user_id = %s
      ''',
                     (user_id, ))

      result = cursor.fetchone()
  return result

def insert_identity(user_id = None,
                    base_url = None,
                    profile_url = None,
                    profile_image = None,
                    username = None,
                    display_name = None,
                    oauth_token = None,
                    oauth_token_secret = None):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
  
      cursor.execute('''
        INSERT INTO identities (user_id,
                                base_url,
                                profile_url,
                                profile_image,
                                username,
                                display_name,
                                oauth_token,
                                oauth_token_secret,
                                last_updated)
        VALUES(%(user_id)s, 
               %(base_url)s, 
               %(profile_url)s,
               %(profile_image)s,
               %(username)s,
               %(display_name)s,
               %(token)s,
               %(secret)s,
               %(time)s)
        ON CONFLICT (profile_url)
        DO UPDATE
        SET
          base_url = %(base_url)s,
          profile_image = %(profile_image)s,
          username = %(username)s,
          display_name =  %(display_name)s,
          oauth_token =  %(token)s,
          oauth_token_secret =  %(secret)s,
          last_updated = %(time)s
        WHERE identities.user_id = %(user_id)s AND identities.profile_url = %(profile_url)s
    ''',
                     {'user_id': user_id,
                      'base_url': base_url,
                      'profile_url': profile_url,
                      'profile_image': profile_image,
                      'username': username,
                      'display_name': display_name,
                      'token': oauth_token,
                      'secret': oauth_token_secret,
                      'time': datetime.now()})

async def async_update_identity(pool: AsyncConnectionPool, identity: gobo_types.Identity):
  async with pool.connection() as aconn:
  #async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
    async with aconn.cursor() as acur:
      await acur.execute('''
      UPDATE identities
        SET
          base_url = %(base_url)s,
          profile_image = %(profile_image)s,
          profile_url = %(profile_url)s,
          username = %(username)s,
          display_name =  %(display_name)s,
          oauth_token =  %(token)s,
          oauth_token_secret =  %(secret)s,
          last_updated = NOW()
        WHERE identities.user_id = %(user_id)s AND identities.identity_id = %(identity_id)s
    ''',
                     {'user_id': identity.user_id,
                      'identity_id': identity.identity_id,
                      'base_url': identity.base_url,
                      'profile_url': identity.profile_url,
                      'profile_image': identity.profile_image,
                      'username': identity.username,
                      'display_name': identity.display_name,
                      'token': identity.oauth_token,
                      'secret': identity.oauth_token_secret})
      
def clear_pending_registration(user_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:     
      cursor.execute('''
        DELETE FROM pending_registrations
        WHERE user_id = %s''', (user_id, ))
      
def get_identity_info(user_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor(row_factory = class_row(gobo_types.Identity)) as cursor:
      cursor.execute('''
        SELECT 
          base_url,
          profile_url, 
          profile_image,
          identity_id,
          username, 
          display_name
        FROM identities
        WHERE user_id = %s''', (user_id, ))
      result = cursor.fetchall()

  return [identity.to_dict() for identity in result]

def delete_identity(user_id, identity_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
      cursor.execute('''
        DELETE FROM identities WHERE user_id = %s AND identity_id = %s;
      ''',
                     (user_id,
                      identity_id, ))

  return


def get_mastodon_credentials(base_url):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor(row_factory = class_row(gobo_types.MastodonCredential)) as cursor:
      cursor.execute('''
        SELECT * FROM mastodon_credentials
        WHERE base_url = %s''',
                     (base_url, ))
      result = cursor.fetchone()
  return result

def insert_mastodon_credentials(base_url, client_id, client_secret):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
  
      cursor.execute('''
        INSERT INTO mastodon_credentials (base_url,
                                          client_id,
                                          client_secret,
                                          last_updated)
        VALUES(%(base_url)s, 
               %(client_id)s, 
               %(client_secret)s,
               %(time)s)
        ON CONFLICT (base_url)
        DO UPDATE
        SET
          client_id = %(client_id)s,
          client_secret = %(client_secret)s,
          last_updated = %(time)s
        WHERE mastodon_credentials.base_url = %(base_url)s
    ''',
                     {'base_url': base_url,
                      'client_id': client_id,
                      'client_secret': client_secret,
                      'time': datetime.now()})
      
def get_blocked_keywords(user_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor(row_factory = class_row(gobo_types.BlockedKeyword)) as cursor:
      cursor.execute('''
        SELECT word, category FROM blocked_keywords
        WHERE user_id = %s''', (user_id, ))
      result = cursor.fetchall()
  return [keyword.to_dict() for keyword in result]


def insert_blocked_keywords(user_id, word, category):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
      try:
        cursor.execute('''
          INSERT INTO blocked_keywords (user_id,
                                        word,
                                        category)
          VALUES(%(user_id)s, %(word)s, %(category)s)''',
                       {"user_id": user_id, "word": word, "category": category})
      except(psycopg.errors.UniqueViolation):
        return "Error: Word and Category combination is already included", 409
      except:
        return "Error: Unkown error", 500
  return "Success", 200
      
def delete_blocked_keywords(user_id, word, category):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
      cursor.execute('''
        DELETE FROM blocked_keywords
        WHERE user_id = %s AND word = %s AND category = %s''', (user_id, word, category))

def get_user_profile(user_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor(row_factory = class_row(gobo_types.UserProfile)) as cursor:
      cursor.execute('''
        SELECT display_name FROM user_profiles
        WHERE user_id = %s''', (user_id, ))
      result = cursor.fetchone()
  if result:
    return result.to_dict()
  else:
    return {"display_name": ""}

def put_user_profile(user_id, display_name):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor() as cursor:
      cursor.execute('''
        INSERT INTO user_profiles (user_id, display_name)
          VALUES (%(user_id)s, %(display_name)s)
        ON CONFLICT (user_id)
        DO UPDATE
        SET
          display_name = %(display_name)s
        WHERE user_profiles.user_id = %(user_id)s''', {"user_id": user_id, "display_name": display_name})

async def async_get_source_subscriptions(pool: AsyncConnectionPool, identity_id = None, list_id = None):
  """Return the set of subscriptions for an input identity_id or list_id. If both identity and list ids are input, will only return identity_id"""
  if identity_id:
    async with pool.connection() as aconn:
      async with aconn.cursor(row_factory = class_row(gobo_types.Subscription)) as acur:
        await acur.execute('''
          SELECT * FROM subscriptions 
          WHERE user_id is NULL AND 
                identity_id = %s AND 
                list_id is NULL AND
                source_id is not NULL
          ORDER BY source_id ASC
        ''', (identity_id, )) 
        result = await acur.fetchall()
        return result
  elif list_id:
    async with pool.connection() as aconn:
      async with aconn.cursor(row_factory = class_row(gobo_types.Subscription)) as acur:
        await acur.execute('''
          SELECT * FROM subscriptions 
          WHERE user_id is NULL AND 
                identity_id is NULL AND 
                list_id = %s AND
                source_id is not NULL
          ORDER BY source_id ASC
        ''', (list_id, )) 
        result = await acur.fetchall()
        return result
  else:
    # This should probably not fail silently
    return []

async def async_get_list_subscriptions(pool: AsyncConnectionPool, identity_id: int = None) -> list:
  if identity_id:
    async with pool.connection() as aconn:
      async with aconn.cursor(row_factory = class_row(gobo_types.Subscription)) as acur:
        await acur.execute('''
          SELECT * FROM subscriptions 
          WHERE user_id is NULL AND 
                identity_id = %s AND 
                list_id is not NULL AND
                source_id is NULL
          ORDER BY list_id ASC
        ''', (identity_id, )) 
        result = await acur.fetchall()
        return result
  else:
    # This should probably not fail silently
    return []
  
async def _insert_followed_source(acur: psycopg.AsyncCursor, followed_source):
  await acur.execute('''
    INSERT INTO followed_sources (last_updated, base_url, identifier, url, username, display_name, icon_url, active)
    VALUES (now(), 
            %(base_url)s, 
            %(identifier)s, 
            %(url)s,
            %(username)s,
            %(display_name)s,
            %(icon_url)s,
            true)
    ON CONFLICT (base_url, identifier) DO UPDATE
    SET
      last_updated = now(),
      url = %(url)s,
      username = %(username)s,
      display_name = %(display_name)s,
      icon_url = %(icon_url)s,
      active = true
    WHERE followed_sources.base_url = %(base_url)s AND followed_sources.identifier = %(identifier)s
    RETURNING *''',
                     {"base_url": followed_source.base_url,
                      "identifier": followed_source.identifier,
                      "url": followed_source.url,
                      "username": followed_source.username,
                      "display_name": followed_source.display_name,
                      "icon_url": followed_source.icon_url})

  result = await acur.fetchone()
  return result

async def async_insert_update_followed_sources(pool: AsyncConnectionPool,
                                               followed_sources: list):
  async with pool.connection() as aconn:
    async with aconn.cursor(row_factory = class_row(gobo_types.FollowedSource)) as acur:
      following = [await _insert_followed_source(acur, source) for source in followed_sources]
  return following

async def _insert_list(acur: psycopg.AsyncCursor, follow_list: gobo_types.List):
  await acur.execute('''
    INSERT INTO lists (user_id, identity_id, base_url, url, identifier, visibility, last_updated, display_name)
    VALUES (%(user_id)s, 
            %(identity_id)s, 
            %(base_url)s,
            %(url)s,
            %(identifier)s,
            %(visibility)s,
            now(),
            %(display_name)s)
    ON CONFLICT (base_url, identifier) DO UPDATE
    SET
      user_id = %(user_id)s, 
      identity_id = %(identity_id)s, 
      url = %(url)s,
      visibility = %(visibility)s,
      last_updated = now(),
      display_name = %(display_name)s
    WHERE lists.base_url = %(base_url)s AND lists.identifier = %(identifier)s
    RETURNING *''',
                     {"user_id": follow_list.user_id,
                      "identity_id": follow_list.identity_id,
                      "base_url": follow_list.base_url,
                      "url": follow_list.url,
                      "identifier": follow_list.identifier,
                      "visibility": follow_list.visibility,
                      "display_name": follow_list.display_name})

  result = await acur.fetchone()
  return result

async def async_insert_update_lists(pool: AsyncConnectionPool,
                                    lists: list) -> list:
  async with pool.connection() as aconn:
    async with aconn.cursor(row_factory = class_row(gobo_types.List)) as acur:
      following = [await _insert_list(acur, follow_list) for follow_list in lists]
  return following

async def _insert_subscription(acur: psycopg.AsyncCursor, subscription: gobo_types.Subscription) -> None:
  #pdb.set_trace()
  await acur.execute('''
    INSERT INTO subscriptions (user_id, identity_id, list_id, source_id, active)
    VALUES (%(user_id)s, %(identity_id)s, %(list_id)s, %(source_id)s, %(active)s)
  ''', {"user_id": subscription.user_id,
        "identity_id": subscription.identity_id,
        "list_id": subscription.list_id,
        "source_id": subscription.source_id,
        "active": subscription.active})

async def _update_subscription(acur: psycopg.AsyncCursor, subscription: gobo_types.Subscription) -> None:
  # This currently does not work. Some of these values will be None
  # The WHERE condition in those cases will end up looking like user_id = NULL which returns NULL according
  # to the SQL spect. Postgres theoretically has an option called transform_null_equals which theoreically fixes
  # this but changing that parameter didn't seem to fix anything so I am not sure.
  await acur.execute('''
    UPDATE subscriptions 
    SET  active = %(active)s 
    WHERE user_id = %(user_id)s AND 
          identity_id = %(identity_id)s AND
          list_id = %(list_id)s AND
          source_id = %(source_id)s  ''',
                     {"user_id": subscription.user_id,
                      "identity_id": subscription.identity_id,
                      "list_id": subscription.list_id,
                      "source_id": subscription.source_id,
                      "active": subscription.active})


async def update_subscriptions(pool: AsyncConnectionPool, subscriptions: list) -> None:
  async with pool.connection() as aconn:
    async with aconn.cursor(row_factory = class_row(gobo_types.List)) as acur:
      for subscription in subscriptions:
        if subscription.is_new:
          await _insert_subscription(acur, subscription)
        else:
          await _update_subscription(acur, subscription)

async def find_identity(pool: AsyncConnectionPool, source: gobo_types.FollowedSource) -> gobo_types.Identity:
  async with pool.connection() as aconn:
    async with aconn.cursor(row_factory = class_row(gobo_types.Identity)) as acur:
      # First look for an identity that is directly subscribed to this source
      
      # If at least one is found, select a random user in that list

      # If none are found, select a random list that is subscribed to this source
      # Then select a random user who is subscribed to that list
      # Repeat until there is a list with a user subscribed

      # If there are still none, just select a random identity from this site
      await acur.execute('''
           SELECT * FROM identities WHERE base_url = %s ORDER BY random() LIMIT 1;
        ''', (source.base_url, )) 
      result = await acur.fetchone()
      return result

async def update_source_last_retrieved(pool: AsyncConnectionPool, source: gobo_types.FollowedSource, retrieved_time: datetime) -> None:
  async with pool.connection() as aconn:
      async with aconn.cursor() as acur:
        await acur.execute('''
          UPDATE followed_sources
          SET last_retrieved = %s
          WHERE source_id = %s''',
                           (retrieved_time, source.source_id))

    
async def insert_post_batch(pool: AsyncConnectionPool, batch: list) -> None:
  async with pool.connection() as aconn:
    async with aconn.cursor() as acur:
      for post in batch:
        await __insert_post(acur, post)
        
async def __insert_post(acur: psycopg.AsyncCursor, post: gobo_types.Post) -> None:
  # TODO(slane): Make this do a proper update. The problem is we don't want to change edited_at unless
  # something has actually changed but for now I'm ignoring that
  await acur.execute('''
    INSERT INTO posts (source_id, base_url, identifier, title, content, author, uri, visibility, retrieved_at)
    VALUES (%(source_id)s,
            %(base_url)s, 
            %(identifier)s,
            %(title)s, 
            %(content)s, 
            %(author)s, 
            %(uri)s, 
            %(visibility)s, 
            %(retrieved_at)s)
    ON CONFLICT (base_url, identifier, source_id) DO NOTHING''',
                     {"source_id": post.source_id,
                      "base_url": post.base_url,
                      "identifier": post.identifier,
                      "title": post.title,
                      "content": post.content,
                      "author": post.author,
                      "uri": post.uri,
                      "visibility": post.visibility,
                      "retrieved_at": post.retrieved_at})
  
def __get_page(page_id, connection):
  with connection.cursor(row_factory = class_row(gobo_types.Page)) as cursor:
    cursor.execute('''SELECT * FROM pages WHERE page_id = %s''', (page_id, ))
    page = cursor.fetchone()
  with connection.cursor(row_factory = class_row(gobo_types.Post)) as cursor:
    # Get the posts associated with this page on page_contents 
    page.posts = cursor.fetchall()

  return page

def __get_newest_page(identity_id, connection):
  with connection.cursor(row_factory = class_row(gobo_types.NewestPage)) as cursor:
    cursor.execute('''SELECT * FROM newest_pages WHERE identity_id = %s''', (identity_id, ))
    newest_page = cursor.fetchone()
  return __get_page(newest_page.page_id, connection)

def get_fresh_feed(user_id):
  with psycopg.connect(conn_str) as connection:
    # Get the set of identities registered to this user
    with connection.cursor(row_factory = class_row(gobo_types.Identity)) as cursor:
      cursor.execute('''SELECT * FROM identities WHERE user_id = %s''', (user_id, ))
      identities = cursor.fetchall()

    # For each identity, get newest page
    return [__get_newest_page(identity.identity_id, connection) for identity in identities]

def __get_recent_posts(identity_id, connection):
  with connection.cursor(row_factory = class_row(gobo_types.Subscription)) as cursor:
    cursor.execute('''
      SELECT * FROM subscriptions 
      WHERE user_id is NULL AND
            identity_id = %s AND
            list_id is NULL AND
            source_id is not NULL''', (identity_id, ))
    subscriptions = cursor.fetchall()

  source_ids = [subscription.source_id for subscription in subscriptions]
  with connection.cursor(row_factory = class_row(gobo_types.Subscription)) as cursor:
    cursor.execute('''
      SELECT * FROM posts
      WHERE source_id = ANY(%s)
    ORDER BY retrieved_at DESC LIMIT 25''', (subscriptions, ))
    posts = cursor.fetchall()
  return posts
    
def get_recent_posts(user_id):
  with psycopg.connect(conn_str) as connection:
    # Get the set of identities registered to this user
    with connection.cursor(row_factory = class_row(gobo_types.Identity)) as cursor:
      cursor.execute('''SELECT * FROM identities WHERE user_id = %s''', (user_id, ))
      identities = cursor.fetchall()

    # For each identity, get newest page
    return [__get_recent_posts(identity.identity_id, connection) for identity in identities]

  
def get_newest_page(identity_id):
  with psycopg.connect(conn_str) as connection:
    with connection.cursor(row_factory = class_row(gobo_types.NewestPage)) as cursor:
      cursor.execute('''
        SELECT * FROM newest_pages WHERE identity_id = %s''', (user_id, ))
      result = cursor.fetchone()
  return [keyword.to_dict() for keyword in result]

def get_page(page_id):
  with psycopg.connect(conn_str) as connection:
    return __get_page(page_id, connection)

