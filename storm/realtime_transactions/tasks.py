from __future__ import absolute_import, print_function, unicode_literals

from invoke import task, run
from streamparse.ext.invoke import *


def pre_submit(topology_name, env_name, env_config):
    """Override this function to perform custom actions prior to topology
    submission. No SSH tunnels will be active when this function is called."""
    pass


def post_submit(topo_name, env_name, env_config):
    """Override this function to perform custom actions after topology
    submission. Note that the SSH tunnel to Nimbus will still be active
    when this function is called."""
    pass
