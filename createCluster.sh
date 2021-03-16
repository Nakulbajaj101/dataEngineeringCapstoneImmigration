#!/bin/bash
clusterName=$1
keyName=$2


######## EMR CLUSTER CONFIGURATION ######################################
aws emr create-default-roles #creating default roles to execute the tasks
aws emr create-cluster \
--applications  Name=Ganglia Name=Spark Name=Zeppelin \
--ec2-attributes '{"KeyName":"'$keyName'", "SubnetId":"subnet-60063d19"}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.28.0 \
--log-uri 's3n://aws-logs-135015496169-us-west-2/elasticmapreduce/' \
--name $clusterName \
--instance-groups '[{"InstanceCount":1, 
                    "InstanceGroupType":"MASTER",
                    "InstanceType":"m5.xlarge",
                    "Name":"Master Instance Group"},

                    {"InstanceCount":4,
                    "InstanceGroupType":"CORE","InstanceType":"m5.xlarge",
                    "Name":"Core Instance Group"}]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-west-2 \
--configurations '[{"Classification":"spark","Properties":{}}]' > "cluster.json"

#wait 10 seconds to fetch the cluster details
sleep 10

clusterId=($(jq -r '.ClusterId' cluster.json))
echo "clusterid is ${clusterId}"


#wait 5 minutes to get the cluster up and running before submitting the job
sleep 300

clusterContent=$(aws emr describe-cluster --cluster-id=${clusterId})
clusterDns=$( jq -r '.Cluster.MasterPublicDnsName' <<< "${clusterContent}" )
echo "clusterDns is ${clusterDns}"

#copying the etl.py file to cluster
scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/$keyName.pem etl.py hadoop@$clusterDns:/home/hadoop/ 
scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/$keyName.pem helperFunctions.py hadoop@$clusterDns:/home/hadoop/ 
scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/$keyName.pem qualityTests.py hadoop@$clusterDns:/home/hadoop/ 
scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/$keyName.pem i94MetadataMappings.py hadoop@$clusterDns:/home/hadoop/ 

#executing the sparkSubmit script over ssh
ssh  -o StrictHostKeyChecking=no -i ~/$keyName.pem hadoop@$clusterDns  'bash -s' < sparkSubmit.sh