import json, logging, os, requests, sqlite3, time
import datetime as dt
from time import sleep



# Node-Watcher is launched from node_watcher_launcher.sh, which provides the following environent variables
try:
  environment       = os.environ['NODE_WATCHER_ENVIRONMENT'] # "dev" or "prod"
  channel           = os.environ['SLACK_CHANNEL']
  token             = os.environ['NODE_WATCHER_TOKEN'] # pasted by user into launcher script
  thread_URI_prefix = os.environ['SLACK_THREAD_URI_PREFIX']
  BIRD_API_prefix   = os.environ['BIRD_API_PREFIX']
  Node_Explorer_API_prefix = os.environ['NODE_EXPORER_API_PREFIX']
except Exception as error:
  print("problem with importing an environment variable, make sure you run this from node_watcher_launcher.sh or node_node_watcher_launcher_dev.sh", error)
  exit(1)


delete_message_URI  = "https://slack.com/api/chat.delete"
get_reactions_URI   = "https://slack.com/api/reactions.get"
post_message_URI    = "https://slack.com/api/chat.postMessage"
node_map_prefix     = "https://www.nycmesh.net/map/nodes/"
http_headers        = {"Content-Type": "application/json; charset=utf-8", "Authorization": "Bearer " + token}



#####################
####   CONFIG    ####
#####################


# different reactions can suppress alert message for different times - "suppress_duration_<slack's-name-of-reaction>_s"
suppress_duration_DATE_s = 86400
suppress_duration_STOPWATCH_s = 10800

# what hour/min the daily report goes out, 24h format, local time
reporting_hour = 9
reporting_minute = 1

# how long before a down node is considered abandoned, and so removed from alerting and reporting
abandoned_threshold_ms = 86400 * 1000 * 14 # 2 weeks

# gonna split logging up between application and network so let's be fancy about it
def setup_logger(name, log_file, log_level):
	formatter = logging.Formatter('%(levelname)s %(message)s')
	handler = logging.FileHandler(log_file)
	handler.setFormatter(formatter)
	logger = logging.getLogger(name)
	logger.setLevel(log_level)
	logger.addHandler(handler)
	return logger


if environment == "prod":
	log_level         = logging.INFO
	application_log   = setup_logger('application_log', './node_watcher.log', log_level) # application-level logs
	node_changes_log  = setup_logger('node_changes_log', './node_changes.log', log_level) # OSPF-level logs
	node_watcher_db 	= "./node-watcher.db"
	alert_time_threshold_ms      = 300000 # how long a node is observed to be down before it goes into alerting state
	hub_down_alert_time_ms       = 180000 # how long a hub is observed as down before alerting - in case we want to be more aggressive about hubs
	error_sleep_time_s           = 10 # how long the main loop waits to run again if there's an error
	hub_watcher_mode             = True # can be disabled for troubleshooting
	hub_down_node_qty            = 5 # how many nodes need to go down at once for the event to be treated as 'hub-down'
	hub_down_raise_qty           = 25 # how many nodes need to go down at once for the event to get raised into other systems e.g. send alerts to other channels
	hub_down_report_interval_s   = 60 # if reporting has been enabled by user, how often reports (of what nodes are still down) go out
	root_cause_guesser_timeout_s = 40 # in case guessing a hub outages root cause gets hung up
	time_rollback_s              = 0 # time machine - good for replaying interesting events


if environment == "dev":
	log_level         = logging.DEBUG
	application_log   = setup_logger('application_log', './node_watcher_dev.log', log_level) # application-level logs
	node_changes_log  = setup_logger('node_changes_log', './node_changes_dev.log', log_level) # OSPF-level logs
	node_watcher_db 	= "./node-watcher-dev.db"
	alert_time_threshold_ms      = 300000 # how long a node is observed to be down before it goes into alerting state
	hub_down_alert_time_ms       = 180000 # how long a hub is observed as down before alerting - in case we want to be more aggressive about hubs
	error_sleep_time_s           = 60 # how long the main loop waits to run again if there's an error 
	hub_watcher_mode             = True # can be disabled for troubleshooting
	hub_down_node_qty            = 5 # how many nodes need to go down at once for the event to be treated as 'hub-down'
	hub_down_raise_qty           = 25 # how many nodes need to go down at once for the event to get raised into other systems e.g. send alerts to other channels
	hub_down_report_interval_s   = 60 # if reporting has been enabled by user, how often reports (of what nodes are still down) go out
	root_cause_guesser_timeout_s = 40 # in case guessing a hub outages root cause gets hung up
	time_rollback_s              = 120 # time machine - good for replaying interesting events



############################
####   HOLIDAY THEMES   ####
############################


# # No holidays, BAU
node_up_emoji = ":point_up:"
node_down_emoji = ":point_down:"

# Halloween
# node_up_emoji = ":jack_o_lantern:"
# node_down_emoji = ":female_zombie:"



##################
####   INIT   ####
##################


conn = sqlite3.connect( node_watcher_db )
db_conn = conn.cursor()

db_conn.execute('CREATE TABLE IF NOT EXISTS slack_threads(node_ip TEXT, thread_ts TEXT)')
db_conn.execute('CREATE INDEX IF NOT EXISTS slack_threads_index ON slack_threads(node_ip)')
db_conn.execute('CREATE TABLE IF NOT EXISTS alert_messages(node_ip TEXT, thread_ts TEXT)')
db_conn.execute('CREATE INDEX IF NOT EXISTS alert_messages_index ON alert_messages(node_ip)')
db_conn.execute('CREATE TABLE IF NOT EXISTS subscriptions(node_ip TEXT PRIMARY KEY, subscribers TEXT DEFAULT (json_array()) NOT NULL )')
db_conn.execute('CREATE INDEX IF NOT EXISTS subscriptions_index ON subscriptions(node_ip)')
conn.commit()


# removed nodes and their timers are tracked here
# to keep state across app restart, you can copy/paste the last state from node_watcher.log 
removed_nodes_tracker = {}
hub_down_tracker = {}
# this is only used during hub-down events, to prevent _many_ API calls (to get emojis).
# referencing this list is quick and API-free at the small cost of it only knows about
# nodes that it has looked up previously (for not hub-down events)
silenced_nodes_cache = ["10.69.3.32", "10.69.48.1"]



#################
###  Filters  ###
#################


# This functionality now happens via the 'x' reaction from within the app, but this will still work
excluded_from_monitoring = []

if environment == "prod":
	def ok_to_monitor( router_id ):
		if router_id not in excluded_from_monitoring \
		and router_id.startswith("10.69") \
		and int(router_id.split('.')[2]) < 80:
			return True
		else:
			return False

if environment == "dev":
	def ok_to_monitor( router_id ):
		return True



#############################################
###  Reaction-Controlled Functionalities  ###
#############################################


def is_silenced( router_id ):

	# First we check the node's thread (in case a user has put reaction there)
	# Then after this we check the (ephemeral) alert message in the main channel
	# Two places that a user could've put a reaction
	query = ('SELECT EXISTS(SELECT * FROM slack_threads WHERE node_ip = ?)')
	thread_exists = db_conn.execute(query, ( router_id,))
	thread_exists = thread_exists.fetchall()

	if thread_exists[0][0]:
		query = 'SELECT * FROM slack_threads WHERE node_ip = ?'
		row = db_conn.execute(query, (router_id,))
		row = row.fetchall()
		thread_ts = row[0][1]

		response = requests.get(get_reactions_URI, headers=http_headers, params={	"channel": channel, "timestamp": thread_ts})
		json_data = response.json()

		if "reactions" in json_data["message"]:

			reactions = []
			for reaction in json_data["message"]["reactions"]:
				reactions.append(reaction["name"])
			if "x" in reactions:
				if router_id not in silenced_nodes_cache:
					silenced_nodes_cache.append(router_id)
				return True
			if any(reaction in reactions for reaction in ["date", "calendar", "stopwatch"]):
				now_s = time.time()
			if any(reaction in reactions for reaction in ["date", "calendar"]):
				if round(now_s) - round(float(thread_ts)) < suppress_duration_DATE_s:
					return True
			if "stopwatch" in reactions:
				if round(now_s) - round(float(thread_ts)) < suppress_duration_STOPWATCH_s:
					return True

	# Now we check the ephemeral alert message in the channel
	# Might not exist so first we check for that
	query = ('SELECT EXISTS(SELECT * FROM alert_messages WHERE node_ip = ?)')
	last_message_exists = db_conn.execute(query, ( router_id, ))
	last_message_exists = last_message_exists.fetchall()

	if last_message_exists[0][0]:
		query = 'SELECT * FROM alert_messages WHERE node_ip = ?'
		row = db_conn.execute(query, ( router_id,))
		row = row.fetchall()
		message_ts = row[0][1]
		response = requests.get(get_reactions_URI, headers=http_headers, params={	"channel": channel, "timestamp": message_ts})
		json_data = response.json()

		if "reactions" in json_data["message"]:

			reactions = []
			for reaction in json_data["message"]["reactions"]:
				reactions.append(reaction["name"])
			if "x" in reactions:
				if router_id not in silenced_nodes_cache:
					silenced_nodes_cache.append(router_id)
				return True
			if any(reaction in reactions for reaction in ["date", "stopwatch"]):
				now_s = time.time()
			if "date" in reactions:
				# print(round(now_s) - round(float(message_ts)), suppress_duration_DATE_s)
				if round(now_s) - round(float(message_ts)) < suppress_duration_DATE_s:
					return True
			if "stopwatch" in reactions:
				if round(now_s) - round(float(message_ts)) < suppress_duration_STOPWATCH_s:
					return True

	if router_id in silenced_nodes_cache:
		silenced_nodes_cache.remove(router_id)
	return False


def get_subscribed_users( router_id ):

	subscribed_users = []
	# First we check the node's thread (in case a user has put reaction there)
	# Then after this we check the (ephemeral) alert message in the main channel
	# Two places that a user could've put a reaction
	query = 'SELECT * FROM slack_threads WHERE node_ip = ?'
	row = db_conn.execute(query, (router_id, ))
	row = row.fetchall()
	thread_ts = row[0][1]
	response = requests.get(get_reactions_URI, headers=http_headers, params={	"channel": channel, "timestamp": thread_ts})
	json_data = response.json()

	if "reactions" in json_data["message"]:

		for reaction in json_data["message"]["reactions"]:

			# "eyes" is a one-shot subscription so no need to check db
			if reaction["name"] == "eyes":  
				for user in reaction["users"]:
					subscribed_users.append( user )

			# Let's first update the subscriptions table to reflect all users' wishes
			if reaction["name"] == "heart":
				query = '''INSERT or IGNORE into subscriptions(node_ip) VALUES(?)'''
				db_conn.execute(query, ( router_id, ))
				for user in reaction["users"]:
					# schema: 'CREATE TABLE IF NOT EXISTS subscriptions(node_ip TEXT PRIMARY KEY, subscribers TEXT DEFAULT (json_array()) NOT NULL )'
					# All this fru-fru does is ensure that unique values get added to the array i.e. no duplicates
					query = ''' UPDATE subscriptions
								SET subscribers = (SELECT json_group_array(DISTINCT value) 
								FROM (SELECT json_insert(subscribers,'$[#]', ?) 
								AS tempArray), json_each(tempArray))
								WHERE node_ip = ? '''
					db_conn.execute(query, ( user, router_id, ))

			if reaction["name"] == "broken_heart":
				# schema: 'CREATE TABLE IF NOT EXISTS subscriptions(node_ip TEXT PRIMARY KEY, subscribers TEXT DEFAULT (json_array()) NOT NULL )'
				# Just to avoid an error from someone mistakenly adding a broken heart when there are no subs for the node
				query = '''INSERT or IGNORE into subscriptions(node_ip) VALUES(?)'''
				db_conn.execute(query, ( router_id, ))
				query = ''' SELECT subscribers from subscriptions, json_each(subscribers) where node_ip = ? '''
				row = db_conn.execute(query, (router_id,))
				row = row.fetchall()
				if row:
					subbed_users_in_db = json.loads(row[0][0])
					for user in reaction["users"]:
						# get index of the user, then remove the user by index
						# TODO: there has to be a better way, someone help me fix this please
						try:
							rem_indx = subbed_users_in_db.index(user)
							query = ''' UPDATE subscriptions
										SET subscribers = json_remove(subscribers, '$[{}]')
										WHERE node_ip = ? '''.format(rem_indx)  
							db_conn.execute(query, (  router_id, ))
						except Exception as e:
							application_log.error('Error', exc_info=e)

		conn.commit()


	# Now we do the same for the (ephemeral) alert message in the channel
	# Might not exist so first we check for that
	query = ('SELECT EXISTS(SELECT * FROM alert_messages WHERE node_ip = ?)')
	last_message_exists = db_conn.execute(query, ( router_id,))
	last_message_exists = last_message_exists.fetchall()

	if last_message_exists[0][0]:

		query = 'SELECT * FROM alert_messages WHERE node_ip = ?'
		row = db_conn.execute(query, (router_id,))
		row = row.fetchall()
		message_ts = row[0][1]
		response = requests.get(get_reactions_URI, headers=http_headers, params={	"channel": channel, "timestamp": message_ts})
		json_data = response.json()

		if "reactions" in json_data["message"]:

			for reaction in json_data["message"]["reactions"]:

				# "eyes" is a one-shot subscription so no need to check db
				if reaction["name"] == "eyes":
					for user in reaction["users"]:
						subscribed_users.append( user )

				# Let's first update the subscriptions table to reflect all users' wishes
				if reaction["name"] == "heart":
					query = '''INSERT or IGNORE into subscriptions(node_ip) VALUES(?)'''
					db_conn.execute(query, ( router_id, ))
					for user in reaction["users"]:
						# schema: 'CREATE TABLE IF NOT EXISTS subscriptions(node_ip TEXT PRIMARY KEY, subscribers TEXT DEFAULT (json_array()) NOT NULL )'
						# All this fru-fru does is ensure that unique values get added to the array i.e. no duplicates
						query = ''' UPDATE subscriptions
									SET subscribers = (SELECT json_group_array(DISTINCT value) 
									FROM (SELECT json_insert(subscribers,'$[#]', ?) 
									AS tempArray), json_each(tempArray))
									WHERE node_ip = ? '''
						db_conn.execute(query, ( user, router_id, ))

				if reaction["name"] == "broken_heart":
					# schema: 'CREATE TABLE IF NOT EXISTS subscriptions(node_ip TEXT PRIMARY KEY, subscribers TEXT DEFAULT (json_array()) NOT NULL )'
					# Just to avoid an error message from someone mistakenly adding a broken heart when there are no subs for the node
					query = '''INSERT or IGNORE into subscriptions(node_ip) VALUES(?)'''
					db_conn.execute(query, ( router_id, ))
					query = ''' SELECT subscribers from subscriptions, json_each(subscribers) where node_ip = ? '''
					row = db_conn.execute(query, (router_id,))
					row = row.fetchall()
					if row:
						subbed_users_in_db = json.loads(row[0][0])
						for user in reaction["users"]:
							# get index of the user, then remove the user by index
							# TODO: there has to be a better way, someone help me fix this please
							try:
								rem_indx = subbed_users_in_db.index(user)
								query = ''' UPDATE subscriptions
											SET subscribers = json_remove(subscribers, '$[{}]')
											WHERE node_ip = ? '''.format(rem_indx)  
								db_conn.execute(query, (  router_id, ))
							except Exception as e:
								application_log.error('Error', exc_info=e)

			conn.commit()

	# now that the db should reflect all users' current wishes, we read from it and report subscriptions
	query = ('SELECT EXISTS(SELECT subscribers FROM subscriptions WHERE node_ip = ?)')
	subscribers_field_exists = db_conn.execute(query, ( router_id, ))
	subscribers_field_exists = subscribers_field_exists.fetchall()
	if subscribers_field_exists[0][0]:
		query = ''' SELECT subscribers from subscriptions, json_each(subscribers) where node_ip = ? '''
		row = db_conn.execute(query, (router_id, ))
		row = row.fetchall()
		if row:
			subbed_users_in_db = json.loads(row[0][0])
			for subbed_user_in_db in subbed_users_in_db:
				subscribed_users.append( subbed_user_in_db )

	return( subscribed_users )



################
####  MISC  ####
################


def get_downtime_humanized( router_id ):
	alert_threshold_m = round(alert_time_threshold_ms / 60000) 
	down_time_m = int(((current_timestamp_ms - removed_nodes_tracker[router_id]["timestamp"])) / 60000)
	# doing this to make things look cleaner from rounding, at the cost of a bit of accuracy
	if down_time_m in [alert_threshold_m - 1, alert_threshold_m, alert_threshold_m + 1]:
		downtime_humanized = str(alert_threshold_m) + " min"
		return ( downtime_humanized )
	if down_time_m < 60:
		downtime_humanized = str(down_time_m) + " min"
	elif 60 <= down_time_m < 2880:
		down_time_h = round((down_time_m / 60), 1)
		downtime_humanized = str(down_time_h) + " hours"
	elif 2880 <= down_time_m:
		down_time_d = round((down_time_m / 1440), 1)
		downtime_humanized = str(down_time_d) + " days"
	return ( downtime_humanized )


def IP_to_NN( IP ):
	NN = None
	if IP.startswith("10.69"):
		if len( IP.split('.')[3] ) == 3:
			NN = int(IP.split('.')[2]) * 100 + int(IP.split('.')[3][1:])
		else:		
			NN = int(IP.split('.')[2]) * 100 + int(IP.split('.')[3])
	return ( NN )


# Gets the most frequent element in a list. If there's a tie, then the
# element that is earliest will be chosen. Very helpful to find the closest
# common upstream node, as node-explorer lists them in order of distance
def most_frequent_and_closest( node_list ):
    highest_count = 0   
    for current_node in node_list:
        current_count = node_list.count(current_node)
        if current_count > highest_count:
            highest_count = current_count
            most_frequent_node = current_node

    return( most_frequent_node )


def get_closest_common_upstream( node_list, before_outage_timestamp ):
	outage_exit_nodes = []
	timeout_s = time.time() + root_cause_guesser_timeout_s
	for router_id in node_list:
		if time.time() > timeout_s:
			application_log.error(f"Node Explorer requests have timed out after {root_cause_guesser_timeout_s} seconds")
			raise Exception("request timeout")
		try:
			Node_Explorer_URI = Node_Explorer_API_prefix + "neighbors/" + router_id
			params = {}
			params["searchDistance"] = "0"
			params["includeEgress"] = "true"
			params["timestamp"] = str(before_outage_timestamp)
			application_log.info(Node_Explorer_URI)
			application_log.debug(f"Node explorer params: {params}")
			response = requests.get(Node_Explorer_URI, params=params)
			json_data = response.json()

			for node in json_data["nodes"]:			
				if node["id"] == router_id:
					exit_path_nodes = node["exit_paths"]["outbound"]

					for exit_path_node in exit_path_nodes:
						outage_exit_nodes.append(exit_path_node[0])

					application_log.debug(f"get_closest_common_upstream: node: {router_id} exit path: {exit_path_nodes}")

		except Exception as e:
			application_log.error(f"get_closest_common_upstream: Error with {router_id}: {e}")

	return( most_frequent_and_closest( outage_exit_nodes ))



def get_hub_down_group_members( hub_down_group ):
	hub_down_group_members = []
	_removed_nodes_tracker = removed_nodes_tracker
	for router_id in _removed_nodes_tracker:
		try:
			if _removed_nodes_tracker[router_id]["hub_down_group"] == hub_down_group:
				hub_down_group_members.append(router_id)
		except:
			pass
	return( hub_down_group_members )


def get_node_webmap_URI( nodes_to_be_mapped ):
	node_map_URI = node_map_prefix
	for node in nodes_to_be_mapped:
		if node != nodes_to_be_mapped[-1]:
			node_map_URI += str(node) + "-"
		else:
			node_map_URI += str(node)
	return(node_map_URI)



#####################
####  MAIN LOOP  ####
#####################


while True:

	# this will keep us roughly in-sync with the BIRD server's cron job
	start_time_s = time.time()

	try:

		##############################
		####  GET DATA FROM BIRD  ####
		##############################

		now = dt.datetime.now(dt.timezone.utc)

		# a_minute_ago's LSDB
		a_minute_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds = 60 + time_rollback_s)
		a_minute_ago_snapshot_suffix = str(a_minute_ago.strftime("%Y/%m/%d/%H/%M") + ".json")
		a_minute_ago_snapshot_URI = BIRD_API_prefix + a_minute_ago_snapshot_suffix
		response = requests.get(a_minute_ago_snapshot_URI)
		deserialized_json = response.json()

		routers = deserialized_json['areas']['0.0.0.0']['routers']
		current_nodes = []
		for ospf_node in routers:
			current_nodes.append(ospf_node)


		# two minutes ago's LSDB
		two_minutes_ago = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds = 120 + time_rollback_s)
		two_minutes_ago_suffix = str(two_minutes_ago.strftime("%Y/%m/%d/%H/%M") + ".json")
		two_minutes_ago_snapshot_URI = BIRD_API_prefix + two_minutes_ago_suffix
		response = requests.get(two_minutes_ago_snapshot_URI)
		deserialized_json = response.json()

		routers = deserialized_json['areas']['0.0.0.0']['routers']
		previous_nodes = []
		for ospf_node in routers:
			previous_nodes.append(ospf_node)


		recently_added_nodes = list(set(current_nodes) - set(previous_nodes))
		recently_removed_nodes = list(set(previous_nodes) - set(current_nodes))



		################################
		#####  LOGIC AND ALERTING  #####
		################################


		current_timestamp_ms = int( time.time() * 1000 ) - int( time_rollback_s * 1000 )

		if recently_added_nodes:

			node_changes_log.info(f"{current_timestamp_ms} Added: {recently_added_nodes}\n")
			
			# In case many nodes in a hub-down event come back up right away,
			# they should all be sent out one request, otherwise there may be 
			# rate-limiting issues. `hub_down_added_nodes` tracks that info across for-loops
			# structure: {<hub down group ID_1>:[list-of-returned-nodes], <hub down group ID_2>:[list-of-returned-nodes]}
			hub_down_added_nodes = {} 

			for router_id in recently_added_nodes:

				if router_id in removed_nodes_tracker and removed_nodes_tracker[router_id]["alerting"] == False:
					removed_nodes_tracker.pop(router_id)

				elif router_id in removed_nodes_tracker and removed_nodes_tracker[router_id]["alerting"] == True \
				and "hub_down_group" not in removed_nodes_tracker[router_id] \
				and is_silenced( router_id ) == False:

					application_log.info(f"{router_id} downtime: {get_downtime_humanized( router_id )}")

					# Get reactions and update subscribed users, before the previous alert message is deleted
					subscribed_users = get_subscribed_users( router_id )
					application_log.info(f"subscribed users: {str(subscribed_users)}" )

					# Delete previous alert message in channel, if exists
					query = ('SELECT EXISTS(SELECT * FROM alert_messages WHERE node_ip = ?)')
					last_message_exists = db_conn.execute(query, ( router_id, ))
					last_message_exists = last_message_exists.fetchall()
					if last_message_exists[0][0]:
						query = 'SELECT * FROM alert_messages WHERE node_ip = ?'
						row = db_conn.execute(query, (router_id,))
						row = row.fetchall()
						thread_ts = row[0][1]
						response = requests.post(delete_message_URI, headers=http_headers, data=json.dumps({ "channel": channel, "ts": thread_ts}))																												
						query = 'DELETE FROM alert_messages WHERE node_ip = ?'
						db_conn.execute(query, (router_id,))

					# Post message to the node's existing thread
					# No need to check if thread exists because it is coming out of alerting
					query     = 'SELECT * FROM slack_threads WHERE node_ip = ?'
					row       = db_conn.execute(query, (router_id,))
					row       = row.fetchall()
					thread_ts = row[0][1]
					body      = (":point_up: " + router_id + " is up! Downtime " + get_downtime_humanized( router_id ) )
					response  = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel , "thread_ts": thread_ts}))

					# Get timestamp from the post above - to be added to main thread message as a link
					json_data       = response.json()
					thread_ts       = json_data["message"]["thread_ts"]
					latest_post_ts  = json_data["message"]["ts"]
					latest_post_URI = 	thread_URI_prefix + channel + "/p" + latest_post_ts.replace('.', '') + "?thread_ts=" + thread_ts + "&cid=" + channel 

					# Post alert message to main channel
					# application_log.info( "is silenced: " )
					# application_log.info( str(is_silenced( router_id ) ))
					body = (node_up_emoji + " " + router_id + " is up! Downtime " + get_downtime_humanized( router_id ) )
					body += " <" + latest_post_URI + "|node history>"
					for user_id in subscribed_users:
						body += " <@" + user_id + "> "						
					application_log.debug(f"node up body: {body}")			
					response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel, "unfurl_links": False }))

					# Get timestamp of main-channel message - to delete it later when new alert goes out
					json_data = response.json()
					thread_ts = json_data["ts"]
					query = 'INSERT into alert_messages(node_ip, thread_ts) VALUES(?,?)'
					db_conn.execute(query, (router_id, thread_ts, ))
					removed_nodes_tracker.pop(router_id)


				elif router_id in removed_nodes_tracker and removed_nodes_tracker[router_id]["alerting"] == True \
				and is_silenced( router_id ) == True:
					removed_nodes_tracker.pop(router_id)

				elif router_id in removed_nodes_tracker and removed_nodes_tracker[router_id]["alerting"] == True \
				and "hub_down_group" in removed_nodes_tracker[router_id] \
				and router_id not in silenced_nodes_cache:

					hub_down_group = removed_nodes_tracker[router_id]["hub_down_group"]
					if not hub_down_group in hub_down_added_nodes:
						hub_down_added_nodes[hub_down_group] = []

					hub_down_added_nodes[hub_down_group].append(router_id)
					# removed_nodes_tracker.pop(router_id)




			if hub_down_added_nodes:

				application_log.info(f"hub_down_added_nodes: {hub_down_added_nodes}")

				for hub_down_group in hub_down_added_nodes:

					query = 'SELECT * FROM slack_threads WHERE node_ip = ?' # TODO rename this node_ip column, or move this data to another table
					row = db_conn.execute(query, (str(hub_down_group),))
					row = row.fetchall()
					thread_ts = row[0][1]
					if len(hub_down_added_nodes[hub_down_group]) == 1:
						body = (":point_up: " + hub_down_added_nodes[hub_down_group][0] + " is up! Downtime " + get_downtime_humanized( hub_down_added_nodes[hub_down_group][0] ) )
					elif len(hub_down_added_nodes[hub_down_group]) > 1:
						body = (":point_up: *These nodes are back up. Their downtime is " + get_downtime_humanized( hub_down_added_nodes[hub_down_group][0])) + ":*\n"
						for router_id in hub_down_added_nodes[hub_down_group]:
							body += router_id + "  "
					response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel , "thread_ts": thread_ts}))

					for router_id in hub_down_added_nodes[hub_down_group]:
						removed_nodes_tracker.pop(router_id)

					if not get_hub_down_group_members( hub_down_group ):
						body = (":sunglasses: all nodes are up" )
						response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel , "thread_ts": thread_ts}))
						hub_down_tracker.pop(hub_down_group)






		if recently_removed_nodes:

			node_changes_log.info(f"Removed: {str( current_timestamp_ms )} {str( recently_removed_nodes )} \n")

			# Need this to decide if this may be a hub-down event
			unsuppressed_qty = 0
			for router_id in recently_removed_nodes:
				if router_id not in silenced_nodes_cache:
					application_log.debug(f"{str(router_id)} not in silenced_nodes_cache")
				else:
					application_log.debug(f"{str(router_id)} _IS_ in silenced_nodes_cache")
				unsuppressed_qty += 1

			if hub_watcher_mode and unsuppressed_qty >= hub_down_node_qty:
				for router_id in recently_removed_nodes:
					# Here we check against the cache in case there are _many_ lookups
					if router_id not in silenced_nodes_cache:
						removed_nodes_tracker[router_id] = {"timestamp" : current_timestamp_ms, "alerting" : False, "hub_down_group": current_timestamp_ms}
			else:
				for router_id in recently_removed_nodes:
					# Here we check against slack which is more accurate
					if ok_to_monitor( router_id ):
						removed_nodes_tracker[router_id] = {"timestamp" : current_timestamp_ms, "alerting" : False}



		if removed_nodes_tracker:

			hub_down_nodes_current = []
			for router_id in removed_nodes_tracker:	

				if current_timestamp_ms - removed_nodes_tracker[router_id]["timestamp"] > alert_time_threshold_ms \
				and removed_nodes_tracker[router_id]["alerting"] == False \
				and "hub_down_group" not in removed_nodes_tracker[router_id] \
				and is_silenced( router_id ) == False:
					# schema: 'CREATE TABLE IF NOT EXISTS slack_threads(node_ip TEXT, thread_ts TEXT)'
					query = ('SELECT EXISTS(SELECT * FROM slack_threads WHERE node_ip = ?)')
					thread_exists = db_conn.execute(query, ( router_id,))
					thread_exists = thread_exists.fetchall()

					if thread_exists[0][0]:						
						# Get reactions and update subscribed users before the last alert message is deleted
						subscribed_users = get_subscribed_users( router_id )
						application_log.info(f"subscribed users: {str(subscribed_users)}")

						# Delete previous alert message
						# The last alert message (in the channel) should always exist if the thread exists, but
						# this hasn't always been the case, as the clean-up functionality was added after the
						# app had been running for some time. The check avoids errors from the earlier versions,
						# and can be eliminated if the App is going into a new Slack channel
						query = ('SELECT EXISTS(SELECT * FROM alert_messages WHERE node_ip = ?)')
						last_message_exists = db_conn.execute(query, ( router_id, ))
						last_message_exists = last_message_exists.fetchall()
						if last_message_exists[0][0]:
							query = 'SELECT * FROM alert_messages WHERE node_ip = ?'
							row = db_conn.execute(query, (router_id, ))
							row = row.fetchall()
							thread_ts = row[0][1]
							response = requests.post(delete_message_URI, headers=http_headers, data=json.dumps({ "channel": channel, "ts": thread_ts}))																												
							query = 'DELETE FROM alert_messages WHERE node_ip = ?'
							db_conn.execute(query, (router_id, ))

						# Post message to the node's existing thread
						query = 'SELECT * FROM slack_threads WHERE node_ip = ?'
						row = db_conn.execute(query, (router_id, ))
						row = row.fetchall()
						thread_ts = row[0][1]
						body = (":point_down: " + router_id + " has been down " + get_downtime_humanized( router_id ))
						response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel , "thread_ts": thread_ts}))

						# Get timestamp from the post above - to be added to main channel message as a link
						json_data = response.json()
						thread_ts = json_data["message"]["thread_ts"]
						latest_post_ts = json_data["message"]["ts"]
						latest_post_URI = 	thread_URI_prefix + channel + "/p" + latest_post_ts.replace('.', '') + "?thread_ts=" + thread_ts + "&cid=" + channel 

						# Post message to main channel
						# application_log.debug(f"{router_id} is silenced: {str(is_silenced(router_id))}")
						body = (node_down_emoji + " " + router_id + " has been down " + get_downtime_humanized( router_id ))
						body += " <" + latest_post_URI + "|node history>"
						for user_id in subscribed_users:
							body += " <@" + user_id + "> "
						response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel, "unfurl_links": False }))

						# Get timestamp of main-channel message - to delete it later when a new alert goes out
						json_data = response.json()
						thread_ts = json_data["ts"]
						query = 'INSERT into alert_messages(node_ip, thread_ts) VALUES(?,?)'
						db_conn.execute(query, (router_id, thread_ts, ))

					else:
						body = (":thread: *" + router_id + "* has been down " + get_downtime_humanized( router_id ))
						response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel}))
						json_data = response.json()
						thread_ts = json_data["ts"]
						query = 'INSERT into slack_threads(node_ip, thread_ts) VALUES(?,?)'
						db_conn.execute(query, (router_id, thread_ts, ))

					removed_nodes_tracker[router_id]["alerting"] = True
					# print(response)


				if current_timestamp_ms - removed_nodes_tracker[router_id]["timestamp"] > hub_down_alert_time_ms \
				and removed_nodes_tracker[router_id]["alerting"] == False \
				and "hub_down_group" in removed_nodes_tracker[router_id] \
				and router_id not in silenced_nodes_cache: # Using cache instead of Slack API call in case there are _many_ lookups 
					hub_down_nodes_current.append( router_id ) 



			application_log.info(f"hub_down_nodes_current: {hub_down_nodes_current}")
			if hub_down_nodes_current and len(hub_down_nodes_current) >= hub_down_node_qty: # need to do this check again in case any nodes have come back up
				hub_down_group = removed_nodes_tracker[hub_down_nodes_current[0]]["timestamp"]

				a_minute_before_outage = round(hub_down_group / 1000) - 60
				two_min_before_outage = round(hub_down_group / 1000) - 120
				try:
					# a subset of nodes is used to speed up the calculation; the distribution in the list should be random enough
					suspected_problem_node = get_closest_common_upstream( hub_down_nodes_current[:10], two_min_before_outage )
				except Exception as e:
					application_log.error('Error', exc_info=e)
					suspected_problem_node = "not sure lol"

				body = ""
				for i in range( round(len(hub_down_nodes_current)/ 5)):
					body += ":fire:"
				body += (" *" + str(len(hub_down_nodes_current)) + "* nodes down at once, looking like a hub went down " + get_downtime_humanized( hub_down_nodes_current[0]) + " ago. ")
				body += ("Suspected root cause node: *" + suspected_problem_node + "*. ")
				body += ("Details and tracking in this here thread :thread:")
				if len(hub_down_nodes_current) >= hub_down_raise_qty:
					body += " <!channel>"
				response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel}))
				json_data = response.json()
				thread_ts = json_data["ts"]
				query = 'INSERT into slack_threads(node_ip, thread_ts) VALUES(?,?)'
				db_conn.execute(query, (hub_down_group, thread_ts, ))

				node_map_URI = node_map_prefix
				nodes_to_be_mapped = []
				body = "*Nodes that are down from this hub outage:*\n"
				for router_id in hub_down_nodes_current:
					body += router_id + "  "
					nodes_to_be_mapped.append(IP_to_NN( router_id ))
					removed_nodes_tracker[router_id]["alerting"] = True

				body += "\n<" + get_node_webmap_URI(nodes_to_be_mapped) + "|Map of down nodes in this outage>"
				response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel , "thread_ts": thread_ts, "unfurl_links": False}))
				hub_down_tracker.update({hub_down_group: {"alerting" : True}})


			if hub_down_nodes_current and len(hub_down_nodes_current) < hub_down_node_qty:
				# in the case that a hub-down event was triggered, but some nodes have come up before time and qty threshhold
				# then don't make a hub event - just remove the hub down group and they'll alert as independant nodes
				for router_id in hub_down_nodes_current:
					del removed_nodes_tracker[router_id]["hub_down_group"]


			if hub_down_tracker:
				application_log.info(f"hub_down_tracker: {hub_down_tracker}")
				for hub_down_group in hub_down_tracker: # in case many hub-down events occur at once :|
					if hub_down_tracker[hub_down_group]["alerting"] == True and int(((current_timestamp_ms / 1000) % hub_down_report_interval_s) / 60) == 0:
						query = 'SELECT * FROM slack_threads WHERE node_ip = ?' # TODO rename this node_ip column, or move this data to another table
						row = db_conn.execute(query, (str(hub_down_group),))
						row = row.fetchall()
						thread_ts = row[0][1]
						response = requests.get(get_reactions_URI, headers=http_headers, params={	"channel": channel, "timestamp": thread_ts})
						json_data = response.json()
						if "reactions" in json_data["message"]:
							for reaction in json_data["message"]["reactions"]:
							# eyes 'turns on' reporting
								if reaction["name"] == "eyes":
									body = (":cry: *Nodes that are still down from this hub outage (enabled by leaving :eyes: reaction on parent):*\n")
									nodes_to_be_mapped = []
									for router_id in get_hub_down_group_members(hub_down_group):
										body += router_id + " "
										nodes_to_be_mapped.append(IP_to_NN( router_id ))
									body += "\n<" + get_node_webmap_URI(nodes_to_be_mapped) + "|Map of nodes that are still down in this outage>"
									response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel , "thread_ts": thread_ts, "unfurl_links": False}))


		# commit changes to database ;)
		conn.commit()



		#######################
		#####  REPORTING  #####
		#######################


		if dt.datetime.today().hour == reporting_hour and dt.datetime.today().minute == reporting_minute:

			abandoned_nodes = []
			for router_id in removed_nodes_tracker:

				if current_timestamp_ms - removed_nodes_tracker[router_id]["timestamp"] > abandoned_threshold_ms:
					abandoned_nodes.append( router_id )
					query = 'SELECT * FROM slack_threads WHERE node_ip = ?'
					row = db_conn.execute(query, (router_id, ))
					row = row.fetchall()
					thread_ts = row[0][1]
					body = (":skull_and_crossbones: " + router_id + " has been down for "  + get_downtime_humanized( router_id ) + " and is now removed from alerting ")
					response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": body, "channel": channel, "thread_ts": thread_ts}))
 
			down_report_summary = ":bar_chart:  Down node report: " + str(len(removed_nodes_tracker) - len(abandoned_nodes)) + " nodes"
			if len( removed_nodes_tracker ) == 0:
				down_report_summary += " :tada:"
			response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": down_report_summary, "channel": channel}))


			if removed_nodes_tracker:
				nodes_to_be_mapped = []
				for router_id in removed_nodes_tracker:
					if removed_nodes_tracker[router_id]["alerting"] == True and not is_silenced( router_id ):
						nodes_to_be_mapped.append( IP_to_NN( router_id ))

					# nodes_to_be_mapped = list(set(nodes_to_be_mapped))

				json_data = response.json()
				thread_ts = json_data["ts"]
				down_report = "```NODE            DOWNTIME        SUPPRESSED \n"
				for router_id in removed_nodes_tracker:
					if router_id not in abandoned_nodes:
						downtime_humanized = get_downtime_humanized( router_id )
						down_report += router_id.ljust(16, " ") + downtime_humanized.ljust(16, " ") + str( is_silenced (router_id) ) + "\n"
				if abandoned_nodes:
					down_report += "\nNodes that have exceeded time limit and are no longer monitored\n(until they show back up in LSDB):\n"
					for router_id in abandoned_nodes:
						downtime_humanized = get_downtime_humanized( router_id )
						down_report += router_id.ljust(16, " ") + downtime_humanized + "\n"
						removed_nodes_tracker.pop( router_id )
				down_report += "```"
				if nodes_to_be_mapped:
					down_report += "\n<" + get_node_webmap_URI(nodes_to_be_mapped) + "|Map of down nodes>"
				response = requests.post(post_message_URI, headers=http_headers, data=json.dumps({  "text": down_report, "channel": channel , "thread_ts": thread_ts, "unfurl_links": False}))


		print(removed_nodes_tracker)
		if time_rollback_s != 0:
			application_log.info(a_minute_ago_snapshot_URI)
		application_log.info(f"{current_timestamp_ms}\n{removed_nodes_tracker}\n{hub_down_tracker}\nsilenced_nodes_cache: {silenced_nodes_cache} \n")
		diff_s = time.time() - start_time_s
		sleep(60 - diff_s) # this keeps us roughly in-sync with the BIRD server's cron job


	except Exception as e:
		application_log.error('Error', exc_info=e)
		application_log.info(a_minute_ago_snapshot_URI)
		application_log.info(f"{current_timestamp_ms}\n{removed_nodes_tracker}\nsilenced_nodes_cache: {silenced_nodes_cache}\n")
		# a potential cause of errors is doing something at the same time that BIRD is, so nudging the time here
		sleep(error_sleep_time_s)
		continue
