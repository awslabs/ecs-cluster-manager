# Copyright 2018 Amazon.com, Inc. or its affiliates.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

import boto3
import json
import time
import base64
import re
from datetime import datetime


def find_cluster_name(ec2_c, instance_id):

    """
    Provided an instance that is currently, or should be part of an ECS cluster
    determines the ECS cluster name.  This is derived from the user-data
    which contains a command to inject the cluster name into ECS agent config
    files.

    On failure we raise an exception which means this instance isn't a ECS
    cluster member so we can proceed with termination.
    """

    response = ec2_c.describe_instance_attribute(
        InstanceId=instance_id,
        Attribute='userData'
    )

    userdata = base64.b64decode(response['UserData']['Value'])

    clustername = re.search("ECS_CLUSTER\s?=\s?(.*?)\s", str(userdata))
    if clustername:
        return(clustername.group(1))

    raise(ValueError(
        "Unable to determine the ECS cluster name from instance metadata"
    ))


def container_instance_healthy(ecs_c, cluster_name, instance_id, context):

    """
    Lists all the instances in the cluster to see if we have one joined
    that matches the instance ID of the one we've just started.

    If we find a cluster member that matches our recently launched instance
    ID, checks whether it's in a status of ACTIVE and shows it's ECS
    agent is connected to the cluster.

    There could be additional checks put in as desired to verify the
    instance is healthy!

    If we're getting short of time waiting for stability return false
    so we can get a continuation.
    """

    while True:

        paginator = ecs_c.get_paginator('list_container_instances')
        instances = paginator.paginate(
            cluster=cluster_name,
            PaginationConfig={
                "PageSize": 10
            }
        )

        for instance in instances:
            response = ecs_c.describe_container_instances(
                cluster=cluster_name,
                containerInstances=instance["containerInstanceArns"]
            )

            for container_instance in response["containerInstances"]:
                if container_instance["ec2InstanceId"] == instance_id:
                    if container_instance["status"] == "ACTIVE":
                        if container_instance["agentConnected"] is True:
                            return(True)

        if context.get_remaining_time_in_millis() <= 40000:
            return(False)

        time.sleep(30)


def find_hook_duration(asg_c, asg_name, instance_id):

    """
    Our Lambda function operates in five-minute time samples, however
    we eventually give up our actions if they take more than 60 minutes.

    This function finds out how long we've been working on our present
    operation by listing current Autoscaling activities, and checking
    for our instance ID to get a datestamp.

    We can then compare that datestamp with present to determine our
    overall duration.
    """

    paginator = asg_c.get_paginator('describe_scaling_activities')

    response_iterator = paginator.paginate(
        AutoScalingGroupName=asg_name,
        PaginationConfig={
            'PageSize': 10,
        }
    )

    hook_start_time = datetime.utcnow()
    for response in response_iterator:
        for activity in response["Activities"]:
            if re.match(
                    "Terminating.*{}".format(instance_id),
                    activity["Description"]
                    ):
                hook_start_time = activity["StartTime"]
                continue

    hook_start_time = hook_start_time.replace(tzinfo=None)

    hook_duration = (datetime.utcnow() - hook_start_time).total_seconds()

    return(int(hook_duration))


def lambda_handler(event, context):

    print("Received event {}".format(json.dumps(event)))

    # Our hook message can look different depending on how we're called.
    # The initial call from AutoScaling has one format, and the call when
    # we send a HeartBeat message has another.  We need to massage them into
    # a consistent format.  We'll follow the format used by AutoScaling
    # versus the HeartBeat message.
    hook_message = {}
    # Identify if this is the AutoScaling call
    if "LifecycleHookName" in event["detail"]:
        hook_message = event["detail"]
    # Otherwise this is a HeartBeat call
    else:
        hook_message = event["detail"]["requestParameters"]
        # Heartbeat comes with instanceId instead of EC2InstanceId
        hook_message["EC2InstanceId"] = hook_message["instanceId"]
        # Our other three elements need to be capitlized
        hook_message["LifecycleHookName"] = hook_message["lifecycleHookName"]
        hook_message["AutoScalingGroupName"] = \
            hook_message["autoScalingGroupName"]
        hook_message["LifecycleActionToken"] = \
            hook_message["lifecycleActionToken"]

    print("Received Lifecycle Hook message {}".format(
        json.dumps(hook_message)
    ))

    try:
        ec2_c = boto3.client('ec2')
        ecs_c = boto3.client('ecs')
        asg_c = boto3.client('autoscaling')

        print("Determining our ECS Cluster name . . .")
        cluster_name = find_cluster_name(
            ec2_c,
            hook_message["EC2InstanceId"]
        )
        print(". . . found ECS Cluster name '{}'".format(
            cluster_name
        ))

        print("Checking status of new instance in the ECS Cluster . . .")
        if container_instance_healthy(
                ecs_c, cluster_name, hook_message["EC2InstanceId"], context
                ):
            print(". . . Instance {} connected and active".format(
                hook_message["EC2InstanceId"]
            ))
            print("Proceeding with instance {} Launch".format(
                hook_message["EC2InstanceId"]
            ))
            asg_c.complete_lifecycle_action(
                LifecycleHookName=hook_message["LifecycleHookName"],
                AutoScalingGroupName=hook_message["AutoScalingGroupName"],
                LifecycleActionToken=hook_message["LifecycleActionToken"],
                LifecycleActionResult="CONTINUE",
                InstanceId=hook_message["EC2InstanceId"]
            )
        else:
            # Figure out how long we've be at this.
            hook_duration = find_hook_duration(
                asg_c,
                hook_message["AutoScalingGroupName"],
                hook_message["EC2InstanceId"]
            )
            print("Determined we cannot proceed with launch.")
            hook_duration = find_hook_duration(
                asg_c,
                hook_message["AutoScalingGroupName"],
                hook_message["EC2InstanceId"]
            )
            print("We've been waiting {} seconds for instance join.".format(
                hook_duration
            ))
            if hook_duration > 3600:
                print("Exceeded 3600 seconds waiting to stabilize.  Aborting")
                asg_c.complete_lifecycle_action(
                    LifecycleHookName=hook_message["LifecycleHookName"],
                    AutoScalingGroupName=hook_message["AutoScalingGroupName"],
                    LifecycleActionToken=hook_message["LifecycleActionToken"],
                    LifecycleActionResult="ABANDON",
                    InstanceId=hook_message["EC2InstanceId"]
                )
            else:
                print("Sending a Heartbeat to continue waiting")
                asg_c.record_lifecycle_action_heartbeat(
                    LifecycleHookName=hook_message["LifecycleHookName"],
                    AutoScalingGroupName=hook_message["AutoScalingGroupName"],
                    LifecycleActionToken=hook_message["LifecycleActionToken"],
                    InstanceId=hook_message["EC2InstanceId"]
                )

    except Exception as e:
        print("Exception: {}".format(e))
        # Exception handling simply involves a raise so we can be retried.
        # CWE should re-try us at least 3 times.  Hopefully the issue resolves
        # next invocation.
        raise
