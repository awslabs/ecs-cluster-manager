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


def find_container_instance_id(ecs_c, cluster_name, instance_id):

    """
    Given an ec2 instance ID determines the cluster instance ID.
    The ec2 instance ID and cluster instance ID aren't the same thing.
    Calls to the ECS control plane require the cluster instance ID.

    I haven't found a 'filter' way to do this so we're left listing
    all container instances and comparing against the known ec2
    instance id.

    On failure we raise an exception which means this instance isn't a ECS
    cluster member so we can proceed with termination.
    """

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
                return(container_instance["containerInstanceArn"])

    raise(ValueError(
        "Unable to determine the ECS Container Instance ID"
    ))


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


def check_stable_cluster(ecs_c, cluster_name, context):

    """
    Goes through all services, and tasks defined against a cluster
    and decides whether they are considered in a stable state.

    For Services we look for a 'service [x] has reached a steady state'
    as the most recent message in the services event list.

    For Tasks we look at the difference between the desired and actual
    states.  If there is a difference the task is not stable.

    When the cluster is finally stable, we will respond true.  If we
    have less than 40 seconds remaining in our Lambda function execution
    time then we will return false so we can send a heartbeat and
    be re-invoked.
    """

    services_stable = False
    tasks_stable = False

    while services_stable is False or tasks_stable is False:

        services_stable = True
        paginator = ecs_c.get_paginator('list_services')
        services = paginator.paginate(
            cluster=cluster_name,
            PaginationConfig={
                "PageSize": 10
            }
        )

        for service in services:
            # Check for no services defined.
            if len(service["serviceArns"]) < 1:
                services_stable = True
                continue

            response = ecs_c.describe_services(
                cluster=cluster_name,
                services=service["serviceArns"]
            )

            for service_status in response["services"]:
                service_ready = False
                if re.search(
                        "service .* has reached a steady state\.",
                        service_status["events"][0]["message"]
                        ):
                    service_ready = True
                    continue

                if service_ready is False:
                    print(" ! Service {} does not appear to be stable".format(
                        service_status["serviceName"]
                    ))
                    services_stable = False

        tasks_stable = True
        paginator = ecs_c.get_paginator('list_tasks')
        tasks = paginator.paginate(
            cluster=cluster_name,
            PaginationConfig={
                "PageSize": 100
            }
        )

        for task in tasks:
            # Check for no tasks defined.
            if len(task["taskArns"]) < 1:
                tasks_stable = True
                continue

            response = ecs_c.describe_tasks(
                cluster=cluster_name,
                tasks=task["taskArns"]
            )

            for task_status in response["tasks"]:
                if task_status["lastStatus"] != task_status["desiredStatus"]:
                    print(
                        " ! Task {} has desired status",
                        " {} with last status {}".format(
                            task_status["taskArn"],
                            task_status["desiredStatus"],
                            task_status["lastStatus"]
                        )
                    )
                    tasks_stable = False

        if context.get_remaining_time_in_millis() <= 40000:
            return(False)

        if services_stable is False or tasks_stable is False:
            time.sleep(30)

    return(True)


def drain_instance(ecs_c, cluster_name, instance_id):

    """
    Marks the ECS container ID that we're set to terminate to DRAIN.
    """

    response = ecs_c.describe_container_instances(
        cluster=cluster_name,
        containerInstances=[
            instance_id
        ]
    )

    if response["containerInstances"][0]["status"] == "ACTIVE":
        ecs_c.update_container_instances_state(
            cluster=cluster_name,
            containerInstances=[
                instance_id
            ],
            status="DRAINING"
        )


def check_instance_drained(ecs_c, cluster_name, instance_id, context):

    """
    Checks and waits until an ECS instance has drained all its running tasks.

    Returns True if the instance drains.

    Returns False if there is less than 40 seconds left in the Lambda
    functions execution and we need to re-invoke to wait longer.
    """

    while True:

        response = ecs_c.describe_container_instances(
            cluster=cluster_name,
            containerInstances=[
                instance_id
            ]
        )

        print("- Instance has {} running tasks and {} pending tasks".format(
            response["containerInstances"][0]["runningTasksCount"],
            response["containerInstances"][0]["pendingTasksCount"]
        ))

        if response["containerInstances"][0]["runningTasksCount"] == 0 and \
                response["containerInstances"][0]["pendingTasksCount"] == 0:
            return(True)

        if context.get_remaining_time_in_millis() <= 40000:
            return(False)

        time.sleep(30)


def lambda_handler(event, context):

    print("Recieved event {}".format(json.dumps(event)))

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

    print("Recieved Lifecycle Hook message {}".format(
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

        print("Translating our EC2 Instance ID into an ECS Instance ID . . .")
        container_instance_id = find_container_instance_id(
            ecs_c,
            cluster_name,
            hook_message["EC2InstanceId"]
        )
        print(". . . found ECS Instance ID '{}'".format(
            container_instance_id
        ))

        proceed_with_termination = False
        print("Setting ECS Instance to drain . . .".format(
            container_instance_id
        ))
        drain_instance(
            ecs_c,
            cluster_name,
            container_instance_id
        )
        print(". . . ECS Instance ID '{}' in DRAINING mode".format(
            container_instance_id
        ))
        print("Confirming ECS Instance has drained all tasks . . .")
        instance_drained = check_instance_drained(
            ecs_c,
            cluster_name,
            container_instance_id,
            context
        )
        if instance_drained is True:
            print(". . . ECS Instance ID '{}' has drained all tasks".format(
                container_instance_id
            ))
            print("Confirming Cluster Services and Tasks are Stable . . .")
            cluster_stable = check_stable_cluster(
                ecs_c,
                cluster_name,
                context
            )
            if cluster_stable is True:
                print(". . . Cluster '{}' appears to be stable".format(
                    cluster_name
                ))
                print("Proceeding with instance id '{}' Termination".format(
                    hook_message["EC2InstanceId"]
                ))
                asg_c.complete_lifecycle_action(
                    LifecycleHookName=hook_message["LifecycleHookName"],
                    AutoScalingGroupName=hook_message["AutoScalingGroupName"],
                    LifecycleActionToken=hook_message["LifecycleActionToken"],
                    LifecycleActionResult="CONTINUE",
                    InstanceId=hook_message["EC2InstanceId"]
                )
                proceed_with_termination = True

        if proceed_with_termination is False:
            print("Determined we cannot proceed with termination.")
            hook_duration = find_hook_duration(
                asg_c,
                hook_message["AutoScalingGroupName"],
                hook_message["EC2InstanceId"]
            )
            print("We've been waiting {} seconds for drain/stabilize.".format(
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
        # Our exception path is to allow the instance to terminate.
        # Exceptions are raised when the instance isn't part of an ECS Cluster
        # already.
        print("Exception: {}".format(e))
        asg_c.complete_lifecycle_action(
            LifecycleHookName=hook_message["LifecycleHookName"],
            AutoScalingGroupName=hook_message["AutoScalingGroupName"],
            LifecycleActionToken=hook_message["LifecycleActionToken"],
            LifecycleActionResult="CONTINUE",
            InstanceId=hook_message["EC2InstanceId"]
        )
