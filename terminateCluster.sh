#/bin/bash
clusterId=($(jq -r '.ClusterId' cluster.json))
aws emr terminate-clusters --cluster-ids $clusterId
rm cluster.json