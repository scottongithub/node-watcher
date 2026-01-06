import json, requests


Node_Explorer_API_prefix = "https://node-explorer.andrew.mesh.nycmesh.net/api/"



def most_frequent_and_first( node_list ):
    highest_count = 0   
    for current_node in node_list:
        current_count = node_list.count(current_node)
        if current_count > highest_count:
            highest_count = current_count
            most_frequent_node = current_node

    return( most_frequent_node )



def get_closest_common_upstream( node_list, timestamp_s ):
	outage_exit_nodes = []
	for router_id in node_list:
		Node_Explorer_URI = Node_Explorer_API_prefix + "neighbors/" + router_id
		params = {}
		params["searchDistance"] = "0"
		params["includeEgress"] = "true"
		if timestamp_s:
			params["timestamp"] = timestamp_s
		response = requests.get(Node_Explorer_URI, params=params)
		json_data = response.json()
		for node in json_data["nodes"]:
			if node["id"] == router_id:
				exit_path_nodes = node["exit_paths"]["outbound"]

				for exit_path_node in exit_path_nodes:
					outage_exit_nodes.append(exit_path_node[0])
					print(exit_path_node[0])

	return( most_frequent_and_first( outage_exit_nodes ))



if __name__ == "__main__":

	node = None
	down_nodes = []
	timestamp_s = input("enter unix timestamp in seconds (defaults to now):\n")
	while node != "":
		node = input("enter node IP address, <enter> twice to do calculation\n")
		down_nodes.append(node)

	down_nodes = down_nodes[:-1]
	print("\ncommon upstream node: " + get_closest_common_upstream( down_nodes, timestamp_s ))

