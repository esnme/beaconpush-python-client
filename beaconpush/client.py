# coding=UTF-8
import re, time, hmac, gevent, logging, hashlib
from zlib import adler32

from beaconpush.socketpool import MultiHostSocketPool
from gevent.greenlet import joinall as join_all_greenlets

logger = logging.getLogger("beaconpush.client")

class IgnoreErrorClient(object):
    def __init__(self, client, error_return_value=None):
        self._client = client
        self._error_return_value = error_return_value

    def __getattr__(self, name):
        real_client_method = getattr(self._client, name)

        def call(*args, **kwargs):
            try:
                return real_client_method(*args, **kwargs)
            except Exception, e:
                logger.warn("Error calling Beaconpush method %s: %s" % (name, e))
                return self._error_return_value

        return call

    # The methods below are overridden in order to have dict-access working. They
    # all delegate to the underlying _client instance. See _routeChannels function below.

    def __hash__(self):
        return hash(self._client)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __cmp__(self, other):
        return cmp(hash(self), hash(other))

class NoClientClient(object):
    def __init__(self, returnValue = None):
        self._returnValue = returnValue

    def __getattr__(self, name):
        def call(*args, **kwargs):
            return self._returnValue

        return call

client_pool = MultiHostSocketPool(connect_timeout=5)

class Invocation(object):
    def __init__(self, host, port, operator, method_name):
        self.host = host
        self.port = port
        self.operator = operator
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        def dispatch():
            client = None
            failed = False

            wait_time = 3
            timeout = gevent.Timeout(wait_time)
            timeout.start()

            addr = (self.host, self.port)
            try:
                client = client_pool.acquire_socket(addr)
                func = getattr(client, self.method_name)
                return func(self.operator, *args, **kwargs)
            except gevent.Timeout, t:
                failed = True
                raise Exception("Beaconpush operation %s timed out after %d seconds" % (self.method_name, wait_time))
            except:
                failed = True
                raise
            finally:
                timeout.cancel()
                client_pool.release_socket(addr, client, failed=failed)

        return gevent.spawn(dispatch)

class ClientProxy(object):
    def __init__(self, host, port, operator):
        self._host = host
        self._port = port
        self._operator = operator

    def __getattribute__(self, name):
        if name[:1] == "_":
            return object.__getattribute__(self, name)

        return Invocation(self._host, self._port, self._operator, name)

USER_ID_PATTERN = re.compile(r"^[a-zA-Z0-9._\-]{1,128}$")
CHANNEL_PATTERN = re.compile(r"^[#\*@]{1,2}[a-zA-Z0-9._\-]{1,128}$")
CHANNEL_PREFIXES = set(["*", "#", "@"])

class Client(object):
    """
    Methods in this class are used to communicate with Beaconpush.
    """
    timeout = 10
    token_ttl = 4320
    clients = []
    port = None
    operator = None
    sign_key = None
    encoder = None

    def __init__(self, hosts, port=6052, operator="default", sign_key="BOGUS_KEY_REPLACE_THIS", encoder=None):
        """
        Creates a new Beaconpush Client instance

        @param hosts: List of beaconpush servers
        @param port: Optional Beaconpush server port. Defaults to 6052
        @param operator: Optional Beaconpush operator. Defaults to "default"
        @param sign_key: Optional Beaconpush sign key. Defaults to "BOGUS_KEY_REPLACE_THIS"
        @param encoder: Optional message encoder, function used to encode messages before sending to Beaconpush server.
                        Should return string. Defaults to None
        """
        self.clients = []
        self.port = port
        self.operator = operator
        self.sign_key = sign_key
        self.encoder = encoder

        for host in hosts:
            self.add_client(host)

    def add_client(self, host):
        """
        add_client(host)

        Adds a new client

        @param host: Host to client
        """
        self.clients.append(ClientProxy(host, self.port, self.operator))
        logger.log(logging.DEBUG, "Beaconpush backend client added: %s" % host)

    def send_to_users(self, message, user_ids):
        """
        send_to_users(message, user_ids)

        Sends a message to multiple users.

        @param message: Arbitrary message to send.
        @param user_ids: List of channel names as strings to receive the message.
        """
        if not type(user_ids) == list:
            user_ids = [user_ids]

        if not user_ids:
            return

        channel_names = [self._user_id_to_channel_name(userId) for userId in user_ids]
        self._send_message(message, channel_names)

    def send_to_channels(self, message, channel_names):
        """
        send_to_channels(message, channel_names)

        Send message to a multiple channels.

        @param message: Arbitrary message to send.
        @param channel_names: Tuple or list of channel names as strings to receive the message.
        """
        if not type(channel_names) == list:
            channel_names = [channel_names]

        for channel_name in channel_names:
            self._validate_channel_prefix(channel_name)

        for channel_name in channel_names:
            self._verify_channel_name(channel_name)

        self._send_message(message, channel_names)

    def _send_message(self, message, channel_names):
        greenlets = []
        clients_and_channels = self._route_channels(channel_names) # {client, [channel_name]}

        if self.encoder is not None:
            message = self.encoder(message)

        for client, channel_names in clients_and_channels.iteritems():
            greenlets.append(client.sendChannelMessage(channel_names, message))

        self._join_all(greenlets)

    def get_users_in_channel(self, channel_name):
        """
        get_users_in_channel(channel_name) -> [string]

        Retrieves a list of users currently subscribing to a channel. The users may or may not
        have actual connections.

        @param channel_name: Channel name as string to get users for.

        @returns A list of all user IDs in the specified channel
        """

        clients = self._get_clients_for_channel(channel_name)
        greenlets = [client.getUsersInChannel(channel_name) for client in clients]
        self._join_all(greenlets)

        resultList = []
        for greenlet in greenlets:
            users = greenlet.get()
            if users is not None:
                resultList += users

        return resultList

    def get_users_online(self, user_ids):
        """
        get_users_online(user_ids) -> [string]

        Query if selected users are online.

        @param user_ids: List of strings representing user IDs that consists of [a-zA-Z0-9._].
        @returns a dict with a boolean mapping for each userId
        """
        if type(user_ids) == list:
            user_ids = [user_ids]

        clients = self._route_users(user_ids)
        greenlets = [client.getUsersOnline(client_user_ids) for client, client_user_ids in clients.items()]
        self._join_all(greenlets)

        result = dict((str(user_id), False) for user_id in user_ids)
        for greenlet in greenlets:
            users_online = greenlet.get()
            if users_online:
                for online_user in users_online:
                    result[online_user] = True

        return result

    def get_num_users_online(self):
        """
        get_num_users_online() -> int

        Get the number of connected users.

        @returns Number of user IDs currently online.
        """
        greenlets = [client.getNumUsersOnline() for client in self._get_clients()]
        self._join_all(greenlets)

        num_online = 0
        for greenlet in greenlets:
            num = greenlet.get()
            if num is not None:
                num_online += num

        return num_online

    def logout(self, user_id):
        """
        logout(userId)

        Force logout for a user. This means closing all of the connections for the user
        and removing the user from the user pool.

        @param user_id: User ID to query, a string representing a userId that consists of [a-zA-Z0-9._].
        """
        for client in self._get_clients():
            client.logout(user_id)

    def generate_token(self, token_id, remote=False):
        """
            @param token_id: user id/channel name
        """
        if remote:
            return self._generate_token_remote(token_id)
        else:
            return self._generate_token_local(token_id)

    def _generate_token_local(self, token_id):
        expire_time = int(time.time()) + self.token_ttl
        payload = "%s;%s" % (token_id, expire_time)
        signature = hmac.new(self.sign_key, payload, hashlib.sha1).hexdigest()
        return "%s;%s" % (expire_time, signature)

    def _generate_token_remote(self, token_id):
        client = self._get_client(token_id)
        return client.generateToken(token_id).get()

    def _user_id_to_channel_name(self, user_id):
        user_id = str(user_id)
        self._verify_user_id(user_id)

        return "@%s" % user_id

    def _get_user_node_index(self, user_id):
        """
        _get_user_node_index(user_id) -> int

        Given a userId, this function returns the node index where the user should connect/is connected.

        @param user_id the user_id
        @return the node index for the given user_id
        """
        try:
            return adler32(str(user_id)) % len(self.clients)
        except:
            return 0

    def _get_clients_for_channel(self, channel_name):
        """
        _get_clients_for_channel(channel_name) -> [client]

        Given a channel_name, this function returns the clients connected to the nodes that handle the channel.
        The length of the list is 1 for local channels and user channels. For a global channel the length is
        equal to the total number of Beaconpush nodes.

        @param channel_name the channel name
        @return a list of clients
        """
        if not channel_name or channel_name[0] == "*":
            # Channel name is empty or a global channel
            return self._get_clients()
        else:
            # A local channel or a personal (user) channel which exists only on one node
            return [self._get_client(channel_name)]

    def _get_clients(self, error_return_value=None):
        clients = []

        # If no Beaconpush client are connected return the NoClientClient
        if not self.clients:
            clients.append(NoClientClient(error_return_value))
            return clients

        for client in self.clients:
            clients.append(IgnoreErrorClient(client, error_return_value))

        return clients

    def _get_client(self, user_id, error_return_value=None):
        if not self.clients:
            return NoClientClient(error_return_value)

        client = self.clients[self._get_user_node_index(user_id)]

        if client in self.clients:
            return IgnoreErrorClient(client, error_return_value)
        else:
            return NoClientClient(error_return_value)

    def _route_channels(self, channel_names):
        """
        _route_channels(channel_names) -> dict

        Groups channels with their appropriate Beaconpush client(s)

        @param channel_names: a list of channel names

        @returns a mapped dict: {client, [channels]}
        """
        routes = {}
        for channel_name in channel_names:
            clients = self._get_clients_for_channel(channel_name)
            for client in clients:
                channels_for_client = routes.get(client, [])
                channels_for_client.append(channel_name)
                routes[client] = channels_for_client
        return routes

    def _route_users(self, user_ids):
        """
        _route_users(user_ids) -> dict

        Groups users with their appropriate Beaconpush client(s)

        @param user_ids: a list of user ids

        @returns a mapped dict: {client, [user_ids]}
        """
        routes = {}
        for user_id in user_ids:
            user_channel = self._user_id_to_channel_name(user_id)
            clients = self._get_clients_for_channel(user_channel)
            for client in clients:
                user_ids_for_client = routes.get(client, [])
                user_ids_for_client.append(user_id)
                routes[client] = user_ids_for_client

        return routes

    def _validate_channel_prefix(self, channel_name):
        if channel_name[0] not in CHANNEL_PREFIXES:
            raise Exception("Beaconpush channel name '%s' has invalid or missing prefix. See documentation for more info." % (channel_name, ))

    def _verify_user_id(self, user_id):
        if not USER_ID_PATTERN.match(user_id):
            raise Exception("Invalid user ID given: '%s'. Must be '%s'. See documentation for more info." % (user_id, USER_ID_PATTERN.pattern))

    def _verify_channel_name(self, channel_name):
        if not CHANNEL_PATTERN.match(channel_name):
            raise Exception("Invalid channel name given: '%s'. Must be '%s'. See documentation for more info." % (channel_name, CHANNEL_PATTERN.pattern))

    def _join_all(self, greenlets):
        """
        Waits for all greenlets or until the thriftTimeout has been reached.

        Note: No exception will be raised from this function. If an error occurs while joining a greenlet, or
        the timeout is reached, the list of greenlets will be emptied. This is in order to ensure that iterating
        the greenlets and calling their get() after this join will be perfectly safe.
        """
        try:
            join_all_greenlets(greenlets, timeout=self.timeout, raise_error=True)
        except:
            logger.exception("Beaconpush result error.")
            del greenlets[:]
