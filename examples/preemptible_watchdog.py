"""This module is an example of automatic run stopped instances."""
#!/usr/bin/env python3

import sys
import time
import logging
import asyncio
import argparse
import yaml
import json
import signal
import os

from yandex_cloud_client import ComputeClient
from yandex_cloud_client.error import YandexCloudError


def sigterm_handler(_signo, _stack_frame):
    logger.info(f"FINISHED: caught signal, shutting down, pid {os.getpid()}")
    sys.exit(0)

signal.signal(signal.SIGINT, sigterm_handler)
signal.signal(signal.SIGTERM, sigterm_handler)


class Config(object):
    """This object represents a settings for snapshotter example.
    `auth_type` sets a way of what token to use, can be `oauth`, `iam`,  `inside-vm` and `sa` (`sa` is default)
    `token` cannot be empty for `oauth`, `iam` types.  If `auth_type` is `inside-vm` iam token gets queried automatically from inside a trusted VM from the matadata service.
    `sa_filepath` is path to SA authorized_key.json file, requried for auth_type `sa`
    `label_go_value` and `label_no_go_value` set the value of `label_name` to filter on, is set both - `label_go_value` wins
    `list of VMs can be givin explicitly using `instances` argument or implicitly, using label-based filter. If given both - instances list form config wins.

    YAML example 1:

    auth_type: oauth
    token: AQAAAAA....
    interval: 15  # seconds
    loglevel: info
    instances:
    - efqwe123qwe123qwe123
    - efzxc456zxc456zxc456

    YAML example 2:

    auth_type: sa
    sa_filepath: /home/user/.keys/authorized_key.json
    interval: 60  # seconds
    loglevel: info
    folder_id: b1gafdpppaiaiu2a4444
    label_name: automation
    label_go_value: keep_vm_up
    label_no_go_value: let_vm_stop

    """

    def __init__(self, filepath: str = None):
        self.filepath = filepath
        self.auth_type =  None
        self.token = args.token or None
        self.sa_filepath = None
        self.interval = args.interval if args.interval is not None else 60
        self.instances = args.instances.split(",") if args.instances else []
        self.loglevel = args.loglevel or "INFO"
        self.folder_id = None
        self.label_name = 'automation'
        self.label_go_value = None # 'keep_vm_up'
        self.label_no_go_value = None # 'let_vm_stop'
        self.log_filename = None
        self.__required_params__ = (self.auth_type, self.interval, self.folder_id)
        self.__build_from_file()
        self.__verify()

        if self.auth_type == "sa":
            self.sa_credentials = self.__read_sa_data_from_file()


    def __read(self):
        try:
            with open(self.filepath, "r") as cfgfile:
                data = yaml.load(cfgfile, Loader=yaml.Loader)
                return data
        except FileNotFoundError:
            raise YandexCloudError(
                f"Config file not found. Please, specify config path: '--config-file <filepath>' "
            )
        except TypeError:
            raise YandexCloudError(
                f"Corrupted config file or bad format. Please, verify your config file: {self.filepath}"
            )

    def __read_sa_data_from_file(self):
        with open(self.sa_filepath, 'r') as infile:
            return json.load(infile)

    def __build_from_file(self):
        # if all(self.__required_params__):
        #     return

        if self.filepath is None:
            return

        config = self.__read()
        if config is None or not config:
            return

        for key, value in config.items():
            if key.lower() == "loglevel":
                value = value.upper()
            setattr(self, key, value)

    def __verify(self):
        if self.auth_type not in ("oauth", "iam", "inside-vm", "sa"):
            raise YandexCloudError('auth_type value is unkown. Known values are "oauth", "iam", "inside-vm", "sa"')
        if self.auth_type in ("auth_type", "iam") and self.token is None:
            raise YandexCloudError("OAuth/IAM token is empty. Please, pass the token by arg '--token' or use config file")
        elif self.auth_type in ("sa") and self.sa_filepath is None:
            raise YandexCloudError("SA filepath is empty. Please, set path to authorized_key.json as 'sa_filepath' in config file")


parser = argparse.ArgumentParser(
    prog="yc-watchdog",
    description="Simple script for automatic start preemptible instances",
    formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=90),
    add_help=False
)

params = parser.add_argument_group("Parameters")
params.add_argument(
    "-t", "--token",
    type=str,
    metavar="str",
    required=False,
    help="Yandex.Cloud OAuth token"
)
params.add_argument(
    "-I", "--interval",
    type=int,
    metavar="int",
    required=False,
    help="Interval for checkout instance state (in seconds). Default: 60"
)
params.add_argument(
    "-i", "--instances",
    type=str,
    metavar="str [, ...]",
    required=False,
    help="Comma separated instances"
)

options = parser.add_argument_group("Options")
options.add_argument(
    "-C", "--config-file",
    metavar="file",
    type=str,
    required=False,
    default="config.yaml",
    help="Path to the config file"
)
options.add_argument(
    "--loglevel",
    metavar="debug/info/warning/error",
    type=str,
    help="Log facility. Default: info"
)
options.add_argument(
    "--help",
    action="help",
    help="Show this help message"
)

args = parser.parse_args()
config = Config(args.config_file)

logging.basicConfig(
    level=config.loglevel.upper(),
    datefmt="%d %b %H:%M:%S",
    format="%(asctime)s %(levelname)s [%(module)s] %(message)s",
    filename=config.log_filename if config.log_filename else None,
    filemode = 'a',
)
logger = logging.getLogger(__name__)

if config.auth_type == "inside-vm":
    compute = ComputeClient(auth_inside_vm=True)
elif config.auth_type == "sa":
    compute = ComputeClient(service_account_key=config.sa_credentials)
elif config.auth_type == "oauth":
    compute = ComputeClient(oauth_token=config.token)
elif config.auth_type == "iam":
    compute = ComputeClient(iam_token=config.token)

last_status_stats = ""

def prep_and_log_statistics(instances ):
    global last_status_stats

    # prepare "total" statistics by status
    instances_dict={}
    for i in instances.values():
        instances_dict[i.status]=instances_dict.get(i.status,0)+1

    # and log statistics if it differs
    new_status_stats = f"{str(instances_dict)}"
    if new_status_stats != last_status_stats:
        logger.info(f"VM statistics: {new_status_stats}")
        last_status_stats=new_status_stats

def get_instances_from_config():
    logger.debug("Validating instances...")
    instances = {}
    for i, instance_id in enumerate(config.instances):
        try:
            instance = compute.instance(instance_id)
            if instance.scheduling_policy.preemptible:
                instances[instance.id]=instance
            else:
                logger.warning(f"removing instance {(instance.id, instance.name)} from watch list, as it's not preemptible")
                config.instances.pop(i)
        except Exception as err:
            logger.error(err)
            config.instances.pop(i)

    return instances

def get_instances_from_yc_folder():
    # TODO: rework and split

    instance_list = compute.instances_in_folder(config.folder_id)

    # filter only preemptible
    instance_list = list(filter(lambda i: i.scheduling_policy.preemptible is True, instance_list))

    # then filter VMs based on go / no-go value from config
    if config.label_go_value:
        instance_list = list(filter(lambda i: i.labels.get(config.label_name)==config.label_go_value, instance_list))
    elif config.label_no_go_value:
        instance_list = list(filter(lambda i: not i.labels.get(config.label_name)==config.label_no_go_value, instance_list))

    return dict(map(lambda i: (i.id, i), instance_list))

async def start_instance(instance):
    if instance.stopped:
        logger.info(f"Instance {instance.name} (id: {instance.id}) stopped. Starting...")
        try:
            await instance.start(run_async_await=True)
            logger.info(f"Instance {instance.name} has been started")
        except YandexCloudError as err:
            logger.error(f"Instance {instance.name} has NOT been started: {err}")
    else:
        logger.debug(f"Unsuitable instance state: {instance.status.lower()}. Skipping...")

def prepare_start_tasks(instances):
    tasks = [
        start_instance(instance)
        for instance in instances
    ]
    logger.debug(f"Created {len(tasks)} tasks for checkout instance state")
    return asyncio.gather(*tasks)


def main():
    logger.info(f"Watchdog is STARTED, pid {os.getpid()}")
    if config.instances:
        get_instances_func =  get_instances_from_config
    else:
        get_instances_func = get_instances_from_yc_folder
        logger.info(f"Instance list gets formed using the filter: label '{config.label_name}', label value '{'(+) ' + config.label_go_value if config.label_go_value else '(-) ' + config.label_no_go_value}'")
    logger.info(f"CONF: [checkout interval set to {config.interval} seconds, instances are watched using {get_instances_func.__name__} func, filename={config.log_filename if config.log_filename else None}]")


    init_instance_list = get_instances_func()
    if init_instance_list:
        logger.info(f"Instance list to watch: {list(map(lambda i: (i.id, i.name), init_instance_list.values())) }")
    else:
        logger.error(f"Instance list is empty, shutting down watchdog")
        sys.exit(0)

    while True:
        all_matching_instances = get_instances_func()
        prep_and_log_statistics(all_matching_instances)
        # keep only stopped ones
        stopped_instances = list(filter(lambda i: i.stopped, all_matching_instances.values()))
        if stopped_instances:
            logger.debug("Preparing tasks...")
            loop = asyncio.get_event_loop()
            loop.run_until_complete(prepare_start_tasks(stopped_instances))
            logger.debug("Tasks completed. Sleeping...")
            loop.close()
        time.sleep(int(config.interval))


if __name__ == "__main__":
    main()
