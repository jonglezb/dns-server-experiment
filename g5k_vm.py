#!/usr/bin/env python3

from __future__ import division, print_function

"""
This python script is built around Execo <http://execo.gforge.inria.fr/doc/latest-stable/>
to run server load experiments on Grid-5000.

The basic idea is to have a powerful server running unbound or bind9, and then
spawn hundreds of VMs, each one opening 50k TCP connections to the server
and performing DNS queries.

The result of the experiment consists in measuring the end-to-end response
time experienced by clients (DNS queries), and the load on the server.
"""

import argparse
import time
import datetime
import os
import sys
import re
import csv
from traceback import format_exc
from copy import deepcopy

import execo
import execo_g5k as g5k
import execo_engine as engine
from execo_engine import logger

import utils


# Example from Matthieu Imbert on the execo-users mailing list
def check_hosts_up(hosts, timeout=None, connection_params=None, polling_interval=5):
    """Check that a list of host are joinable with ssh.

    Checks that all hosts of the list are joinable with ssh. Retry
    continuously to connect to them every <polling_interval> seconds,
    until either all are reachable or the timeout is reached. Returns
    the list of hosts which are joinable.

    :param hosts: list of hosts

    :param timeout: timeout of the checks. No timeout if None.

    :param connection_params: to connect to the hosts. Note that the
      ssh_option entry of the connection_params is overwritten by this
      function

    :param polling_interval: tries to connect each <polling_interval>
      seconds.

    :returns: list of joinable hosts
    """

    start_ts = time.time()
    if timeout != None:
        completion_ts = start_ts + timeout
    remaining_hosts = set(hosts)
    if connection_params != None:
        real_connection_params = connection_params
    else:
        real_connection_params = {}
    while len(remaining_hosts) > 0 and (timeout == None or time.time() <= completion_ts):
        #print('remaining_hosts=%s' % (remaining_hosts,))
        if timeout != None:
            next_poll_ts = min(time.time() + polling_interval, completion_ts)
        else:
            next_poll_ts = time.time() + polling_interval
        poll_timeout = max(0, next_poll_ts - time.time())
        real_connection_params.update({'ssh_options': ( '-tt',
                                                        '-o', 'BatchMode=yes',
                                                        '-o', 'PasswordAuthentication=no',
                                                        '-o', 'StrictHostKeyChecking=no',
                                                        '-o', 'UserKnownHostsFile=/dev/null',
                                                        '-o', 'ConnectTimeout=%s' % (int(poll_timeout),))})
        check = execo.Remote('true',
                             remaining_hosts,
                             connection_params=real_connection_params,
                             process_args={'timeout': poll_timeout,
                                           'nolog_exit_code': True,
                                           'nolog_timeout': True}).run()
        hosts_up = [ p.host for p in check.processes if p.finished_ok ]
        #print('hosts_up=%s' %(hosts_up,))
        remaining_hosts = remaining_hosts.difference(hosts_up)
        if len(remaining_hosts) > 0:
            execo.sleep(max(0, next_poll_ts - time.time()))
    return list(set(hosts).difference(remaining_hosts))


class DNSServerExperiment(engine.Engine):
    def __init__(self):
        super(DNSServerExperiment, self).__init__()
        ## Parse command-line arguments
        self.args_parser.add_argument('--mode', choices=['udp', 'tcp', 'tls'], default='tcp',
                            help='Whether to run in UDP, TCP or TLS mode (default: %(default)s)')
        self.args_parser.add_argument('--tls-keytype', choices=['rsa:2048', 'rsa:4096'], default='rsa:2048',
                            help='Type of private key to generate for TLS operation (default: %(default)s)')
        self.args_parser.add_argument('--vmhosts-site',
                            help='Which Grid5000 site (i.e. frontend) to use for the VM hosts (default: local site).')
        self.args_parser.add_argument('--vmhosts-cluster',
                            help='Which Grid5000 cluster to use for the VM hosts (default: any cluster).  Unused if -j is passed.')
        self.args_parser.add_argument('--server-site',
                            help='Which Grid5000 site (i.e. frontend) to use for the server (default: local site).')
        self.args_parser.add_argument('--server-cluster',
                            help='Which Grid5000 cluster to use for the server (default: any cluster).  Unused if -J is passed.')
        self.args_parser.add_argument('--cluster',
                            help='Shortcut for both --server-cluster and --vmhosts-cluster.')
        self.args_parser.add_argument('--production', action="store_true",
                            help='Allow to use a production cluster')
        self.args_parser.add_argument('--nb-hosts', '-N', type=int, default=2,
                            help='Number of physical machines to reserve on the cluster to run VMs (default: %(default)s).  Unused if -j is passed.')
        self.args_parser.add_argument('--start-date', '-r',
                            help='Start OAR jobs at the given date, instead of right now')
        self.args_parser.add_argument('--container-job',
                            help='Run all OAR jobs inside the given container job ID')
        self.args_parser.add_argument('--vmhosts-job-id', '-j', type=int,
                            help='Instead of making a reservation for VM hosts, use an existing OAR job ID')
        self.args_parser.add_argument('--server-job-id', '-J', type=int,
                            help='Instead of making a reservation for the server, use an existing OAR job ID')
        self.args_parser.add_argument('--subnet-job-id', '-S', type=int,
                            help='Instead of making a reservation for a subnet, use an existing OAR job ID')
        self.args_parser.add_argument('--server-env', '-e',
                            help='Name of the Kadeploy environment (OS image) to deploy on the server.  Can be a filename or the name of a registered environment.')
        self.args_parser.add_argument('--vmhosts-env', default='debian9-x64-std',
                            help='Name of the Kadeploy environment (OS image) to deploy on VM hosts.  Can be a filename or the name of a registered environment.  Default: %(default)s')
        self.args_parser.add_argument('--vmhosts-kadeploy-user', default='deploy',
                            help='Kadeploy username, used when passing the name of a registered environment as VM host environment (default: %(default)s)')
        self.args_parser.add_argument('--kadeploy-user', '-u', default='deploy',
                            help='Kadeploy username, used when passing the name of a registered environment as server environment (default: %(default)s)')
        self.args_parser.add_argument('--vm-image', '-i', required=True,
                            help='Path to the qcow2 VM image to use (on the G5K frontend)')
        self.args_parser.add_argument('--nb-vm', '-n', type=int, default=1,
                            help='Number of VM to spawn on each VM host (default: %(default)s)')
        self.args_parser.add_argument('--memory', '-m', type=int, default=2048,
                            help='Memory in MB to allocate to each VM (default: %(default)s)')
        self.args_parser.add_argument('--walltime', '-t', type=int,
                            help='How much time the reservations should last, in seconds.  Unused if -j, -J and -S are passed.')
        self.args_parser.add_argument('--random-seed', '-s', type=int,
                            help='Random seed for tcpclient/udpclient, for reproducibility (default: current date)')
        self.args_parser.add_argument('--client-duration', '-T', type=int, required=True,
                            help='Duration, in seconds, for which to run the TCP or UDP clients (unused if query rate uses the complex format)')
        self.args_parser.add_argument('--client-query-rate', '-Q', type=utils.int_or_rates_duration,
                            help='Number of queries per second for each client (VM).  Either a single integer (rate), or a comma-separated list of (rate, duration) couples.  Example: "1000 8000ms, 2000 5s" means 1k qps during 8 seconds, then 2k qps during 5 seconds.')
        self.args_parser.add_argument('--client-query-rate-linear', type=utils.int_or_rates_duration,
                            help='Sequence of query rate increase or decrease (linear slopes) for each client (VM).  Comma-separated list of (slope, duration) couples.  Example: "700 8000ms, -400 5s" means a +700 qps/s increase during 8 seconds, then -400 qps/s during 5 seconds.')
        self.args_parser.add_argument('--client-connection-rate', '-q', type=int, required=True,
                            help='Number of new connections per second that each client (VM) will open')
        self.args_parser.add_argument('--client-connections', '-C', type=int, required=True,
                            help='Number of TCP or UDP connections opened by each client (VM)')
        self.args_parser.add_argument('--resolver', choices=['unbound', 'bind9', 'knot-resolver'],
                            default='unbound',
                            help='Which resolver to use (default: %(default)s)')
        self.args_parser.add_argument('--bind9-version', default='9.13.3',
                            help='Which version of bind9 to use (default: %(default)s)')
        self.args_parser.add_argument('--knot-version', default='3.0.0',
                            help='Which version of knot resolver to use (default: %(default)s)')
        self.args_parser.add_argument('--server-threads', type=int, default=32,
                            help='Number of server threads to use for the resolver (default: %(default)s)')
        self.args_parser.add_argument('--resolver-slots-per-thread', type=int,
                            help='Sets the number of client slots to allocate per resolver thread (by default, a reasonable value is computed in TCP mode)')
        self.args_parser.add_argument('--cpunetlog-interval', default="0.1",
                            help='Interval in seconds between each cpunetlog data collection (default: %(default)s)')

    def init(self):
        # Initialise random seed if not passed as argument
        if self.args.random_seed == None:
            now = datetime.datetime.timestamp(datetime.datetime.now())
            self.args.random_seed = int(now)
        # "Experiment ID", used in OAR job names
        self.exp_id = "{}".format(self.args.random_seed)[-5:]
        # Various ways of specifying query rate
        if self.args.client_query_rate_linear != None and self.args.client_query_rate == None:
            logger.error("Error: --client-query-rate-linear must be used with an initial query rate -Q")
            raise Exception
        self.simple_queryrate = isinstance(self.args.client_query_rate, int) and self.args.client_query_rate_linear == None
        self.stdin_queryrate = not isinstance(self.args.client_query_rate, int)
        self.stdin_queryratelinear = self.args.client_query_rate_linear != None
        # Compute number of unbound slots if not set
        if self.args.resolver_slots_per_thread == None:
            self.args.resolver_slots_per_thread = 500 + int(1.05 * self.args.client_connections * self.args.nb_hosts * self.args.nb_vm / self.args.server_threads)
        # Don't use any TCP slots in UDP mode
        if self.args.mode == 'udp':
            self.args.resolver_slots_per_thread = 0
        ## Physical machines
        # Backwards compatibility
        if self.args.cluster:
            self.args.server_cluster = self.args.vmhosts_cluster = self.args.cluster
        # OAR job for VM hosts, represented as (oarjob ID, frontend)
        self.vmhosts_job = None
        # List of machines (execo.host.Host) to be used to host VMs
        self.vm_hosts = []
        # OAR job for the server, represented as (oarjob ID, frontend)
        self.server_job = None
        # Machine (execo.host.Host) to be used as server in the experiment
        self.server = None
        self.server_port = 853 if self.args.mode == 'tls' else 53
        self.server_conn_params = deepcopy(execo.default_connection_params)
        self.server_conn_params.update({'user': 'root'})
        utils.disable_pty(self.server_conn_params)
        ## Network
        # Global VLAN for multisite experiment
        self.global_vlan = None
        # OAR job for subnet, represented as (oarjob ID, frontend)
        self.subnet_job = None
        # Subnet used by the VMs (as CIDR)
        self.subnet = None
        # List of all (IP, MAC) available on the subnet
        self.subnet_ip_mac = []
        ## VM
        # MAC and IP addresses assigned to VM
        self.vm_macs = []
        self.vm_ips = []
        # List of VM, as Host instances
        self.vm = []
        # Process that runs all VMs, as a "Remote" instance
        self.vm_process = None
        # Name of resolver, mostly for logging
        self.resolver_name = ""
        if self.args.resolver == 'unbound':
            self.resolver_name = 'unbound 1.6.7'
        elif self.args.resolver == 'bind9':
            self.resolver_name = 'bind {}'.format(self.args.bind9_version)
        elif self.args.resolver == 'knot-resolver':
            self.resolver_name = 'knot-resolver {}'.format(self.args.knot_version)

    def multi_site(self):
        if self.args.vmhosts_site == None and self.args.server_site == None:
            return False
        return self.args.vmhosts_site != self.args.server_site

    def reserve_global_vlan(self):
        """Global VLAN, only used for multi-site experiment (server not on the
        same site as the VM)"""
        # TODO: integrate that into the "single job reservation" thing.
        # Existing job, look in all currently running jobs
        for (job_id, frontend) in g5k.get_current_oar_jobs():
            vlans = g5k.get_oar_job_kavlan(job_id, frontend)
            if len(vlans) > 0:
                logger.debug("Found existing Kavlan job {} (VLAN ID: {})".format(job_id, vlans[0]))
                self.globalvlan_job = (job_id, frontend)
                return
        # New job
        submission = g5k.OarSubmission(resources="{{type='kavlan-global'}}/vlan=1",
                                       name="VLAN {}".format(self.exp_id),
                                       reservation_date=self.args.start_date,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission, None)])
        self.globalvlan_job = (jobid, site)

    def reserve_resources_singlejob(self):
        """Reserve subnet, server and VM hosts in a single OAR job.  Only used for
        single-site experiment."""
        assert(not self.multi_site())
        subnet_resource = "slash_22=1"
        if self.args.vmhosts_cluster == self.args.server_cluster:
            # Single reservation for both VM hosts and server, to ensure
            # they are on the same switch.
            machines_resources = "{}switch=1/nodes={}".format(utils.cluster_oarstr(self.args.vmhosts_cluster),
                                                              self.args.nb_hosts + 1)
        else:
            # Reserve server and VM hosts in different clusters
            server_resources = "{}switch=1/nodes=1".format(utils.cluster_oarstr(self.args.server_cluster))
            vmhosts_resources = "{}switch=1/nodes=1".format(utils.cluster_oarstr(self.args.vmhosts_cluster),
	                                                    self.args.nb_hosts)
            machines_resources = "+".join([server_resources, vmhosts_resources])
        # Aggregate all resources in a single reservation
        resources = "+".join([subnet_resource, machines_resources])
        # Determine job type
        job_type = ["deploy"]
        if self.args.container_job != None:
            job_type.append("inner={}".format(self.args.container_job))
        if self.args.production:
            job_type.append("production")
        submission = g5k.OarSubmission(resources=resources,
                                       name="Combined {}".format(self.exp_id),
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission, self.args.vmhosts_site)])
        # Single job
        self.vmhosts_job = self.server_job = self.subnet_job = (jobid, site)

    def reserve_subnet(self):
        # Existing job
        if self.args.subnet_job_id:
            self.subnet_job = (self.args.subnet_job_id, self.args.vmhosts_site)
            return
        # New job
        if self.args.container_job != None:
            job_type = "inner={}".format(self.args.container_job)
        else:
            job_type = None
        submission = g5k.OarSubmission(resources="slash_22=1", name="Subnet {}".format(self.exp_id),
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission, self.args.vmhosts_site)])
        self.subnet_job = (jobid, site)

    def reserve_vmhosts(self):
        # Existing job
        if self.args.vmhosts_job_id:
            self.vmhosts_job = (self.args.vmhosts_job_id, self.args.vmhosts_site)
            return
        # New job
        if self.args.container_job != None:
            job_type = "inner={}".format(self.args.container_job)
        else:
            job_type = None
        if self.args.vmhosts_cluster:
            resources = "{{cluster='{}'}}/switch=1/nodes={}".format(self.args.vmhosts_cluster,
                                                                    self.args.nb_hosts)
        else:
            resources = "switch=1/nodes={}".format(self.args.nb_hosts)
        submission = g5k.OarSubmission(resources=resources, name="VM hosts {}".format(self.exp_id),
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission, self.args.vmhosts_site)])
        self.vmhosts_job = (jobid, site)

    def reserve_server(self):
        # Existing job
        if self.args.server_job_id:
            self.server_job = (self.args.server_job_id, self.args.server_site)
            return
        # New job
        if self.args.container_job != None:
            job_type = ["deploy", "inner={}".format(self.args.container_job)]
        else:
            job_type = "deploy"
        if self.args.server_cluster:
            resources = "{{cluster='{}'}}/switch=1/nodes=1".format(self.args.server_cluster)
        else:
            resources = "switch=1/nodes=1"
        submission = g5k.OarSubmission(resources=resources, name="Server {}".format(self.exp_id),
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission, self.args.server_site)])
        self.server_job = (jobid, site)

    def prepare_global_vlan(self):
        vlans = g5k.get_oar_job_kavlan(*self.globalvlan_job)
        if len(vlans) > 0:
            self.global_vlan = vlans[0]
            logger.debug("Global VLAN ID: {}".format(self.global_vlan))
        else:
            logger.error("Could not reserve global VLAN")
            sys.exit(1)

    def prepare_subnet(self):
        # subnet_params is a dict: http://execo.gforge.inria.fr/doc/latest-stable/execo_g5k.html#get-oar-job-subnets
        (ip_mac_list, subnet_params) = g5k.get_oar_job_subnets(*self.subnet_job)
        self.subnet = subnet_params['ip_prefix']
        self.subnet_ip_mac = ip_mac_list

    def start_deploy_vmhosts(self):
        hosts = g5k.get_oar_job_nodes(*self.vmhosts_job)
        if self.multi_site():
            self.vm_hosts = hosts
        else:
            # Take all but the first host
            self.vm_hosts = sorted(hosts, key=lambda node: node.address)[1:]
        if os.path.isfile(self.args.vmhosts_env):
            deploy_opts = {"env_file": self.args.vmhosts_env}
        else:
            deploy_opts = {"env_name": self.args.vmhosts_env,
                           "user": self.args.vmhosts_kadeploy_user}
        if self.multi_site():
            deploy_opts["vlan"] = self.global_vlan
            logger.debug("Deploying environment '{}' on {} VM hosts in VLAN {}...".format(self.args.vmhosts_env,
                                                                                          len(self.vm_hosts),
                                                                                          self.global_vlan))
        else:
            logger.debug("Deploying environment '{}' on {} VM hosts...".format(self.args.vmhosts_env,
                                                                               len(self.vm_hosts)))
        d = g5k.Deployment(self.vm_hosts, **deploy_opts)
        return g5k.kadeploy.Kadeployer(d).start()

    def finish_deploy_vmhosts(self, deploy_process):
        deployed = deploy_process.deployed_hosts
        if len(deployed) != len(self.vm_hosts):
            logger.error("Could not deploy all VM hosts, only {}/{} deployed".format(len(deployed), len(self.vm_hosts)))
            sys.exit(1)
        if self.multi_site():
            logger.debug("Deployed, transforming VM hosts name to be able to reach them in the new VLAN")
            for host in self.vm_hosts:
                host.address = g5k.get_kavlan_host_name(host.address, self.global_vlan)

    def prepare_vmhosts(self):
        script = """\
# Avoid conntrack on all machines
iptables -t raw -A PREROUTING -p tcp -j NOTRACK
iptables -t raw -A PREROUTING -p udp -j NOTRACK
iptables -t raw -A OUTPUT -p tcp -j NOTRACK
iptables -t raw -A OUTPUT -p udp -j NOTRACK
        """
        task = execo.Remote(script,
                            self.vm_hosts,
                            connection_params=self.server_conn_params).start()
        return task

    def start_all_vm(self):
        """Starts VM on reserved machines, and returns the associated task
        object.  This function will return immediately, but the caller has to
        wait for the VM to be setup before using them.
        """
        assert(len(self.vm_hosts) > 0)
        (all_ip, all_mac) = zip(*self.subnet_ip_mac)
        self.vm_macs = all_mac[:self.args.nb_vm*len(self.vm_hosts)]
        self.vm_ips = all_ip[:self.args.nb_vm*len(self.vm_hosts)]
        logger.debug("VMs IP: {}".format(' '.join(self.vm_ips)))
        memory = self.args.memory
        nb_vm = self.args.nb_vm
        # For each physical host, build a list of MAC addresses to be used for its VMs
        macs_per_host = [self.vm_macs[i*nb_vm:(i+1)*nb_vm] for i, host in enumerate(self.vm_hosts)]
        # Double escaping is magic (after .format, it will become {{macs_per_host}})
        script = """\
for mac in {{{{[' '.join(macs) for macs in macs_per_host]}}}}
do
  iface=$(tunctl -b)
  brctl addif br0 "$iface"
  ip link set "$iface" up
  kvm -m {memory} -smp cores={cores},threads=1,sockets=1 -nographic -localtime -enable-kvm -drive file="{image}",if=virtio,media=disk -snapshot -net nic,model=virtio,macaddr="$mac" -net tap,ifname="$iface",script=no &
done
wait
        """.format(memory=memory, cores=1, image=self.args.vm_image)
        vm_task = execo.Remote(script, self.vm_hosts, connection_params=self.server_conn_params, name="Run VM on all hosts")
        return vm_task.start()

    def start_deploy_server(self):
        """Deploy the server with the given Kadeploy environment.  Blocks until
        deployment is done"""
        # Sort hosts by name and take the first one: this is useful when
        # using a single reservation for all nodes, since we will always
        # pick the same host as server.
        self.server = sorted(g5k.get_oar_job_nodes(*self.server_job), key=lambda node: node.address)[0]
        if os.path.isfile(self.args.server_env):
            deploy_opts = {"env_file": self.args.server_env}
        else:
            deploy_opts = {"env_name": self.args.server_env,
                           "user": self.args.kadeploy_user}
        if self.multi_site():
            deploy_opts["vlan"] = self.global_vlan
            logger.debug("Deploying environment '{}' on server {} in VLAN {}...".format(self.args.server_env,
                                                                                        self.server.address,
                                                                                        self.global_vlan))
        else:
            logger.debug("Deploying environment '{}' on server {}...".format(self.args.server_env,
                                                                             self.server.address))
        d = g5k.Deployment([self.server], **deploy_opts)
        return g5k.kadeploy.Kadeployer(d).start()

    def finish_deploy_server(self, deploy_process):
        deployed = deploy_process.deployed_hosts
        if len(deployed) == 0:
            logger.error("Could not deploy server")
            sys.exit(1)
        if self.multi_site():
            logger.debug("Deployed, transforming {} into {}".format(self.server.address, g5k.get_kavlan_host_name(self.server.address, self.global_vlan)))
            self.server.address = g5k.get_kavlan_host_name(self.server.address, self.global_vlan)

    def prepare_server(self):
        # At this point, the server is already deployed
        bind_version = "v" + self.args.bind9_version.replace(".", "_")
        knot_version = "v" + self.args.knot_version
        script = """\
rc=0
# Add direct route to VM network
ip route replace {vm_subnet} dev eth0 || rc=$?

# Increase max number of incoming connections
sysctl net.ipv4.tcp_syncookies=0 || rc=$?
sysctl net.core.somaxconn=100000 || rc=$?
sysctl net.ipv4.tcp_max_syn_backlog=100000 || rc=$?
sysctl fs.file-max=20000000 || rc=$?
echo 20000000 > /proc/sys/fs/nr_open || rc=$?

[ "{resolver}" = "unbound" ] && {{
# Update git repository for unbound.
cd /root/unbound || rc=$?
git pull || rc=$?
make -j8 || rc=$?
}}

[ "{resolver}" = "bind9" ] && {{
# Install bind
cd /root/
[ -d "bind9" ] || git clone  https://gitlab.isc.org/isc-projects/bind9.git || rc=$?
cd bind9 || rc=$?
git pull || rc=$?
git checkout {bind_version}
# Perf tuning: https://kb.isc.org/docs/aa-01314
# We can't tune the buffer size, it is unconditionally set to 16 MB by --with-tuning=large.
# -DRCVBUFSIZE=4194304
./configure --with-tuning=large --enable-largefile --enable-shared --enable-static --with-openssl=/usr --with-gnu-ld --with-atf=no --disable-linux-caps 'CFLAGS=-O2 -fstack-protector-strong -Wformat -Werror=format-security -fno-strict-aliasing -fno-delete-null-pointer-checks -DNO_VERSION_DATE -DDIG_SIGCHASE' 'LDFLAGS=-Wl,-z,relro -Wl,-z,now' 'CPPFLAGS=-Wdate-time -D_FORTIFY_SOURCE=2' || rc=$?
make -j32 || rc=$?
}}

[ "{resolver}" = "knot-resolver" ] && {{
# Install knot-resolver
apt-get update
cd /root/
[ -d "knot-resolver" ] || git clone  https://gitlab.labs.nic.cz/knot/knot-resolver.git || rc=$?
cd knot-resolver || rc=$?
git pull || rc=$?
git checkout {knot_version} || rc=$?
git submodule update --init --recursive || rc=$?
apt-get --yes install -t stretch-backports libknot-dev || rc=$?
apt-get --yes build-dep -t stretch-backports knot-resolver || rc=$?
make -j32 CFLAGS="-DNDEBUG" daemon modules || rc=$?
make install || rc=$?
}}

# Install CPUNetLog
apt-get --yes install python3 python3-psutil python3-netifaces
cd /root/
[ -d "CPUnetLOG" ] || git clone https://github.com/jonglezb/CPUnetLOG || rc=$?
cd CPUnetLOG || rc=$?
git pull || rc=$?
exit $rc
        """.format(vm_subnet=self.subnet,
                   resolver=self.args.resolver,
                   bind_version=bind_version,
                   knot_version=knot_version)
        task = execo.Remote(script, [self.server],
                            connection_params=self.server_conn_params,
                            name="Setup server").start()
        return task

    def start_cpunetlog(self, hosts, conn_params=None):
        script = """/root/CPUnetLOG/__init__.py --stdout -i {}""".format(self.args.cpunetlog_interval)
        if conn_params == None:
            conn_params = deepcopy(execo.default_connection_params)
        else:
            conn_params = deepcopy(conn_params)
        utils.disable_pty(conn_params)
        task = execo.Remote(script, hosts,
                            connection_params=conn_params,
                            name="CPUnetLOG").start()
        return task

    def wait_until_vm_ready(self):
        prospective_vms = [execo.Host(ip, user='root') for ip in self.vm_ips]
        logger.debug('Waiting for {} VMs to become reachable...'.format(len(prospective_vms)))
        self.vm = check_hosts_up(prospective_vms, timeout=60)
        logger.debug('Result: {} VMs are reachable.'.format(len(self.vm)))

    def kill_all_vm(self):
        task = execo.Remote("killall qemu-system-x86_64 || true", self.vm_hosts,
                            connection_params=g5k.default_oarsh_oarcp_params,
                            name="Kill all VMs")
        task.run()

    def prepare_vm(self):
        script = """\
rc=0
# Install dependencies
apt-get update || rc=$?
apt-get --yes install libssl-dev || rc=$?

# Update git repository for tcpclient.
cd /root/tcpscaler || rc=$?
git pull || rc=$?
make || rc=$?

# Add direct route to server (but don't fail if it fails).
# We use the old-style "route" because it can resolve DNS names, unlike "ip route"
route add {server_name} eth0 || echo "Failed to add route to {server_name}" >&2

# Increase max number of outgoing connections
sysctl net.ipv4.ip_local_port_range="1024 65535" || rc=$?
sysctl net.ipv4.tcp_tw_reuse=1 || rc=$?

# No connection tracking
iptables -t raw -A PREROUTING -p tcp -j NOTRACK || rc=$?
iptables -t raw -A PREROUTING -p udp -j NOTRACK || rc=$?
iptables -t raw -A OUTPUT -p tcp -j NOTRACK || rc=$?
iptables -t raw -A OUTPUT -p udp -j NOTRACK || rc=$?

# Install CPUNetLog
cd /root/
apt-get --yes install python3 python3-psutil python3-netifaces || rc=$?
[ -d "CPUnetLOG" ] || git clone https://github.com/jonglezb/CPUnetLOG || rc=$?
cd CPUnetLOG || rc=$?
git pull || rc=$?

exit $rc
        """.format(server_name=self.server.address)
        conn_params = deepcopy(execo.default_connection_params)
        utils.disable_pty(conn_params)
        task = execo.Remote(script, self.vm, name="Setup VM",
                            connection_params=conn_params).start()
        return task

    def configure_unbound(self, params):
        unbound_config = """\
cat > /tmp/unbound.conf <<EOF
server:
  interface: 0.0.0.0
  port: {port}
  access-control: 0.0.0.0/0 allow
  username: root
  use-syslog: no
  chroot: ""
  directory: "."
  pidfile: "/tmp/unbound.pid"
  incoming-num-tcp: {max_tcp_clients_per_thread}
  num-threads: {nb_threads}
  msg-buffer-size: {buffer_size}
  so-rcvbuf: 4m
  so-reuseport: yes
  local-zone: example.com static
  local-data: "example.com A 42.42.42.42"
EOF
        """.format(**params)
        if self.args.mode == 'tls':
            unbound_config += """\
cat >> /tmp/unbound.conf <<EOF
  ssl-service-key: /tmp/resolver.key
  ssl-service-pem: /tmp/resolver.cert
EOF
            """
        return unbound_config

    def configure_bind9(self, params):
        bind_config = """
cat > /tmp/named.conf <<EOF
options {{
  port {port};
  pid-file none;
  recursion yes;
  allow-recursion {{ any; }};
  recursive-clients 10000;
  tcp-clients {max_tcp_clients_per_thread};
  tcp-listen-queue 8192;
  reserved-sockets {max_tcp_clients_per_thread};
}};
zone "example.com"  {{ type master; file "/tmp/db.example.com"; }};
EOF

cat > /tmp/db.example.com <<EOF
\$TTL	86400
@	IN	SOA	localhost. root.localhost. (
			      1		; Serial
			 604800		; Refresh
			  86400		; Retry
			2419200		; Expire
			  86400 )	; Negative Cache TTL
;
@	IN	NS	localhost.
@	IN	A	42.42.42.42
EOF
        """.format(**params)
        return bind_config

    def configure_knot(self, params):
        knot_config = """
cat > /tmp/knot-resolver.conf <<EOF
modules = {{
  'hints',
}}

-- knot resolver automatically enables TLS when listening on port 853
net.listen('0.0.0.0', {port})

hints['example.com'] = '42.42.42.42'
EOF
        """.format(**params)
        if self.args.mode == 'tls':
            knot_config += """\
cat >> /tmp/knot-resolver.conf <<EOF
net.tls("/tmp/resolver.cert", "/tmp/resolver.key")
EOF
            """
        return knot_config

    def start_dns_server(self):
        resolver_params = {
            "resolver": self.args.resolver,
            "buffer_size": 4096,
            "nb_threads": self.args.server_threads,
            "max_tcp_clients_per_thread": self.args.resolver_slots_per_thread,
            # Only used by bind9: according to the documentation, reserved-sockets can be at most maxsockets - 128
            "maxsockets": self.args.resolver_slots_per_thread + 128,
            "mode": self.args.mode,
            "port": 853 if self.args.mode == 'tls' else 53,
        }
        max_clients = self.args.server_threads * self.args.resolver_slots_per_thread
        logger.debug("{resolver} in {mode} mode using {nb_threads} threads, {max_tcp_clients_per_thread} max TCP/TLS clients per thread, {buffer_size}b buffer size".format(**resolver_params))
        logger.debug("Max TCP/TLS clients: {}".format(max_clients))
        if self.args.resolver == 'unbound':
            resolver_config = self.configure_unbound(resolver_params)
        elif self.args.resolver == 'bind9':
            resolver_config = self.configure_bind9(resolver_params)
        elif self.args.resolver == 'knot-resolver':
            resolver_config = self.configure_knot(resolver_params)
        execo.Remote(resolver_config, [self.server],
                     connection_params=self.server_conn_params,
                     name="Configure resolver").run()
        # Generate TLS key and self-signed certificate
        if self.args.mode == 'tls':
            generate_tls = "openssl req -x509 -subj '/CN=localhost' -nodes -newkey {} -keyout /tmp/resolver.key -out /tmp/resolver.cert -days 365".format(self.args.tls_keytype)
            execo.Remote(generate_tls, [self.server],
                         connection_params=self.server_conn_params,
                         name="Generate TLS key and cert").run()
        # Run resolver
        if self.args.resolver == 'unbound':
            resolver_cmd = "pkill unbound; sleep 3; /root/unbound/unbound -d -v -c /tmp/unbound.conf"
        elif self.args.resolver == 'bind9':
            resolver_cmd = "/root/bind9/bin/named/named -c /tmp/named.conf -g -n {nb_threads} -U {nb_threads} -S {maxsockets}"
        elif self.args.resolver == 'knot-resolver':
            resolver_cmd = "LD_LIBRARY_PATH=/usr/local/lib /usr/local/sbin/kresd -f {nb_threads} -c /tmp/knot-resolver.conf"

        task = execo.Remote(resolver_cmd.format(**resolver_params),
                            [self.server],
                            connection_params=self.server_conn_params,
                            name="Resolver process").start()
        return task

    def start_client_vm(self):
        """Start tcpclient or udpclient on all VM"""
        if self.args.mode == 'tcp':
            client = 'tcpclient'
        elif self.args.mode == 'tls':
            client = 'tcpclient --tls'
        else:
            client = 'udpclient'
        # Create a different random seed for each client, but
        # deterministically based on the global seed.
        random_seed = [self.args.random_seed + vm_id for vm_id, vm in enumerate(self.vm)]
        # Disable PTY allocation
        conn_params = deepcopy(execo.default_connection_params)
        utils.disable_pty(conn_params)
        if self.simple_queryrate:
            script = "/root/tcpscaler/{} -s {{{{random_seed}}}} -t {} -R -p {} -r {} -c {} -n {} {}"
            script = script.format(client, self.args.client_duration,
                                   self.server_port,
                                   self.args.client_query_rate,
                                   self.args.client_connections,
                                   self.args.client_connection_rate,
                                   self.server.address)
        elif self.stdin_queryrate:
            script = "/root/tcpscaler/{} -s {{{{random_seed}}}} --stdin -R -p {} -c {} -n {} {}"
            script = script.format(client, self.server_port, self.args.client_connections,
                                   self.args.client_connection_rate,
                                   self.server.address)
        elif self.stdin_queryratelinear:
            script = "/root/tcpscaler/{} -s {{{{random_seed}}}} --stdin-rateslope -R -p {} -r {} -c {} -n {} {}"
            script = script.format(client, self.server_port, self.args.client_query_rate,
                                   self.args.client_connections,
                                   self.args.client_connection_rate,
                                   self.server.address)
        task = execo.Remote(script, self.vm, name=client, connection_params=conn_params).start()
        if self.stdin_queryrate:
            # Write desired query rate sequence to stdin of all processes
            task.write("{}\n".format(len(self.args.client_query_rate)).encode())
            for rateduration in self.args.client_query_rate:
                task.write("{} {}\n".format(rateduration.duration_ms, rateduration.rate).encode())
        if self.stdin_queryratelinear:
            # Write desired query rate increase/decrease sequence to stdin of all processes
            task.write("{}\n".format(len(self.args.client_query_rate_linear)).encode())
            for rateslope_duration in self.args.client_query_rate_linear:
                task.write("{} {}\n".format(rateslope_duration.duration_ms, rateslope_duration.rate).encode())
        return task

    def log_experimental_conditions(self):
        logger.info("Random seed: {}".format(self.args.random_seed))
        logger.info("Subnet [job {}]: {}".format(self.subnet_job[0],
                                                 self.subnet))
        all_hosts = sorted([s.address for s in g5k.get_oar_job_nodes(*self.vmhosts_job)])
        logger.info("{} machines [job {}]: {}".format(len(all_hosts),
                                                      self.vmhosts_job[0],
                                                      ' '.join(all_hosts)))

    def log_output(self, task, task_name, log_stdout=True, log_stderr=True):
        logger.debug("Logging stdout/stderr of task {} ({} processes)".format(task_name, len(task.processes)))
        for process_id, process in enumerate(task.processes):
            if len(task.processes) > 1:
                stdout_file = self.result_dir + "/{}_{}_stdout".format(task_name, process_id)
                stderr_file = self.result_dir + "/{}_{}_stderr".format(task_name, process_id)
            else:
                stdout_file = self.result_dir + "/{}_stdout".format(task_name)
                stderr_file = self.result_dir + "/{}_stderr".format(task_name)
            if log_stdout:
                with open(stdout_file, 'w') as stdout:
                    stdout.write(process.stdout)
            if log_stderr:
                with open(stderr_file, 'w') as stderr:
                    stderr.write(process.stderr)

    def run(self):
        rtt_file = self.result_dir + "/rtt.csv"
        resolver = None
        client = 'tcpclient' if self.args.mode == 'tcp' else 'udpclient'
        try:
            logger.debug("Experiment ID: {}".format(self.exp_id))
            if self.multi_site():
                logger.info("Running in multi-site mode")
            if not self.multi_site():
                self.reserve_resources_singlejob()
                logger.debug("Waiting for OAR job to start...")
                g5k.wait_oar_job_start(*self.vmhosts_job)
                self.prepare_subnet()
                logger.debug("Prepared subnet")
            # Dependencies (besides the obvious ones):
            # - deploy_server depends on prepare_global_vlan
            # - prepare_server depends on deploy_server
            # - prepare_server depends on prepare_subnet
            # - prepare_vm depends on deploy_server
            if self.multi_site():
                self.reserve_global_vlan()
                g5k.wait_oar_job_start(*self.globalvlan_job)
                logger.debug("Waiting for global VLAN job to start...")
                self.prepare_global_vlan()
            self.log_experimental_conditions()
            logger.debug("Deploying VM hosts...")
            machines_deploy_process = self.start_deploy_vmhosts()
            logger.debug("Deploying server image...")
            server_deploy_process = self.start_deploy_server()
            machines_deploy_process.wait()
            logger.debug("Finishing deploying VM hosts...")
            self.finish_deploy_vmhosts(machines_deploy_process)
            logger.debug("Setting up VM hosts...")
            machines_setup_process = self.prepare_vmhosts()
            machines_setup_process.wait()
            logger.debug("VM hosts are setup.")
            server_deploy_process.wait()
            logger.debug("Finishing deploying server...")
            self.finish_deploy_server(server_deploy_process)
            logger.debug("Server is deployed.")
            self.vm_process = self.start_all_vm()
            # Ensure VM are killed when we exit
            with self.vm_process:
                server_setup_process = self.prepare_server()
                self.wait_until_vm_ready()
                vm_setup_process = self.prepare_vm()
                server_setup_process.wait()
                self.log_output(server_setup_process, "server_setup_process")
                if not server_setup_process.ok:
                    logger.error("Error while preparing server, please check logs for 'server_setup_process'")
                    raise Exception
                logger.debug("Prepared server: {}".format(self.server.address))
                vm_setup_process.wait()
                self.log_output(vm_setup_process, "vm_setup_process")
                if not vm_setup_process.ok:
                    logger.error("Error while preparing VMs, please check logs for 'vm_setup_process'")
                    raise Exception
                logger.debug("Prepared VM")
                logger.info("Started {} VMs.".format(len(self.vm)))
                cpunetlog_vms = self.start_cpunetlog(self.vm)
                cpunetlog_server = self.start_cpunetlog([self.server], self.server_conn_params)
                resolver = self.start_dns_server()
                logger.info("Started resolver ({}) on {}.".format(self.resolver_name, self.server.address))
                # Leave time for resolver to start
                if self.args.resolver_slots_per_thread < 1000000:
                    execo.sleep(15)
                else:
                    execo.sleep(60)
                logger.info("Starting {} on all VMs...".format(client))
                clients = self.start_client_vm()
                clients.wait()
                logger.info("{} finished!".format(client))
                logger.info("Writing cpunetlog output to disk.")
                cpunetlog_server.kill().wait()
                cpunetlog_vms.kill().wait()
                self.log_output(cpunetlog_server, "cpunetlog_server")
                self.log_output(cpunetlog_vms, "cpunetlog_vms")
                logger.info("writing {} results to disk.".format(client))
                self.log_output(clients, "clients", log_stdout=False)
                with open(rtt_file, 'w') as rtt_output:
                    need_header = True
                    rtt = csv.writer(rtt_output)
                    for client_id, client in enumerate(clients.processes):
                        first_line = True
                        for line in iter(client.stdout.splitlines()):
                            # Skip anything that does not look like CSV
                            if ',' not in line:
                                continue
                            if need_header:
                                # Take CSV header from first client and add a column
                                data = line.split(",")
                                data.insert(0, "vm_id")
                                rtt.writerow(data)
                                need_header = False
                                first_line = False
                            elif first_line:
                                # Skip first line of subsequent clients
                                first_line = False
                            else:
                                # Add column with VM ID
                                data = line.split(",")
                                data.insert(0, client_id)
                                rtt.writerow(data)

        except Exception as e:
            logger.error("Exception raised: {}\n{}".format(e, format_exc()))
        finally:
            #self.kill_all_vm()
            if self.vm_process:
                self.vm_process.kill()
            if resolver:
                resolver.kill()
                logger.debug("Waiting for resolver to exit")
                resolver.wait()
                self.log_output(resolver, "resolver")
            if self.vm_process:
                logger.debug("Waiting for VM to exit")
                self.vm_process.wait()
                logger.info("Resolver and all VMs are shut down")
                self.log_output(self.vm_process, "vm_process")
                print(execo.Report([self.vm_process]).to_string())
            #for s in self.vm_process.processes:
            #    print("\n%s\nstdout:\n%s\nstderr:\n%s\n" % (s, s.stdout, s.stderr))
            g5k.oardel([self.vmhosts_job])


if __name__ == "__main__":
    engine = DNSServerExperiment()
    engine.start()
