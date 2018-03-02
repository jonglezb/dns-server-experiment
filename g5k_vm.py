#!/usr/bin/env python3

from __future__ import division, print_function

"""
This python script is built around Execo <http://execo.gforge.inria.fr/doc/latest-stable/>
to run server load experiments on Grid-5000.

The basic idea is to have a powerful server running unbound, and then
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

import execo
import execo_g5k as g5k
import execo_engine as engine
from execo_engine import logger


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
        self.args_parser.add_argument('--cluster',
                            help='Which Grid5000 cluster to use (defaut: any cluster).  Unused if -j and -J are passed.')
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
        self.args_parser.add_argument('--kadeploy-user', '-u',
                            help='Kadeploy username, used when passing the name of a registered environment as server environment')
        self.args_parser.add_argument('--server-threads', type=int, default=32,
                            help='Number of server threads to use for unbound (default: %(default)s)')
        self.args_parser.add_argument('--vm-image', '-i', required=True,
                            help='Path to the qcow2 VM image to use (on the G5K frontend)')
        self.args_parser.add_argument('--nb-vm', '-n', type=int, default=1,
                            help='Number of VM to spawn on each VM host (default: %(default)s)')
        self.args_parser.add_argument('--memory', '-m', type=int, default=2048,
                            help='Memory in MB to allocate to each VM (default: %(default)s)')
        self.args_parser.add_argument('--walltime', '-t', type=int,
                            help='How much time the reservations should last, in seconds.  Unused if -j, -J and -S are passed.')
        self.args_parser.add_argument('--random-seed', '-s', type=int,
                            help='Random seed for tcpclient, for reproducibility (default: current date)')
        self.args_parser.add_argument('--client-duration', '-T', type=int, required=True,
                            help='Duration, in seconds, for which to run the TCP clients')
        self.args_parser.add_argument('--client-query-rate', '-Q', type=int, required=True,
                            help='Number of queries per second for each client (VM)')
        self.args_parser.add_argument('--client-connection-rate', '-q', type=int, required=True,
                            help='Number of new connections per second that each client (VM) will open')
        self.args_parser.add_argument('--client-connections', '-C', type=int, required=True,
                            help='Number of TCP connections opened by each client (VM)')
        self.args_parser.add_argument('--unbound-slots-per-thread', type=int,
                            help='Sets the number of client slots to allocate per unbound thread (by default, a reasonable value is computed)')

    def init(self):
        # Initialise random seed if not passed as argument
        if self.args.random_seed == None:
            now = datetime.datetime.timestamp(datetime.datetime.now())
            self.args.random_seed = int(now)
        # Compute number of unbound slots if not set
        if self.args.unbound_slots_per_thread == None:
            self.args.unbound_slots_per_thread = 500 + int(1.05 * self.args.client_connections * self.args.nb_hosts * self.args.nb_vm / self.args.server_threads)
        ## Physical machines
        # OAR job for VM hosts, represented as (oarjob ID, frontend)
        self.vmhosts_job = None
        # List of machines (execo.host.Host) to be used to host VMs
        self.vm_hosts = []
        # OAR job for the server, represented as (oarjob ID, frontend)
        self.server_job = None
        # Machine (execo.host.Host) to be used as server in the experiment
        self.server = None
        self.server_conn_params = execo.default_connection_params
        self.server_conn_params.update({'user': 'root'})
        ## Network
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

    def reserve_subnet(self):
        # Existing job
        if self.args.subnet_job_id:
            self.subnet_job = (self.args.subnet_job_id, None)
            return
        # New job
        if self.args.container_job != None:
            job_type = "inner={}".format(self.args.container_job)
        else:
            job_type = None
        submission = g5k.OarSubmission(resources="slash_22=1", name="VM subnet",
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission , None)])
        self.subnet_job = (jobid, site)

    def reserve_vmhosts(self):
        # Existing job
        if self.args.vmhosts_job_id:
            self.vmhosts_job = (self.args.vmhosts_job_id, None)
            return
        # New job
        if self.args.container_job != None:
            job_type = "inner={}".format(self.args.container_job)
        else:
            job_type = None
        if self.args.cluster:
            resources = "{{cluster='{}'}}/switch=1/nodes={}".format(self.args.cluster,
                                                                    self.args.nb_hosts)
        else:
            resources = "switch=1/nodes={}".format(self.args.nb_hosts)
        submission = g5k.OarSubmission(resources=resources, name="VM hosts",
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission , None)])
        self.vmhosts_job = (jobid, site)

    def reserve_server(self):
        # Existing job
        if self.args.server_job_id:
            self.server_job = (self.args.server_job_id, None)
            return
        # New job
        if self.args.container_job != None:
            job_type = ["deploy", "inner={}".format(self.args.container_job)]
        else:
            job_type = "deploy"
        if self.args.cluster:
            resources = "{{cluster='{}'}}/switch=1/nodes=1".format(self.args.cluster)
        else:
            resources = "switch=1/nodes=1"
        submission = g5k.OarSubmission(resources=resources, name="Server",
                                       reservation_date=self.args.start_date,
                                       job_type=job_type,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission , None)])
        self.server_job = (jobid, site)

    def prepare_subnet(self):
        # subnet_params is a dict: http://execo.gforge.inria.fr/doc/latest-stable/execo_g5k.html#get-oar-job-subnets
        (ip_mac_list, subnet_params) = g5k.get_oar_job_subnets(*self.subnet_job)
        self.subnet = subnet_params['ip_prefix']
        self.subnet_ip_mac = ip_mac_list

    def prepare_vmhosts(self):
        self.vm_hosts = g5k.get_oar_job_nodes(*self.vmhosts_job)
        script = """\
# Avoid conntrack on all machines
sudo-g5k iptables -t raw -A PREROUTING -p tcp -j NOTRACK
sudo-g5k iptables -t raw -A OUTPUT -p tcp -j NOTRACK

# Create br0 if it does not yet exist
ip link show dev br0 || {
  sudo-g5k brctl addbr br0
  sudo-g5k brctl addif br0 eth1
  sudo-g5k ip link set br0 up
}
        """
        task = execo.Remote(script,
                            self.vm_hosts,
                            connection_params=g5k.default_oarsh_oarcp_params).start()
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
  iface=$(sudo-g5k create_tap)
  kvm -m {memory} -smp cores={cores},threads=1,sockets=1 -nographic -localtime -enable-kvm -drive file="{image}",if=virtio,media=disk -snapshot -net nic,model=virtio,macaddr="$mac" -net tap,ifname="$iface",script=no &
done
wait
        """.format(memory=memory, cores=1, image=self.args.vm_image)
        vm_task = execo.Remote(script, self.vm_hosts, connection_params=g5k.default_oarsh_oarcp_params, name="Run VM on all hosts")
        return vm_task.start()

    def deploy_server(self):
        """Deploy the server with the given Kadeploy environment.  Blocks until
        deployment is done"""
        # Sort servers by name and take the first one: if several servers
        # are part of the OAR job, this ensures we always pick the same one.
        self.server = sorted(g5k.get_oar_job_nodes(*self.server_job), key=lambda node: node.address)[0]
        if os.path.isfile(self.args.server_env):
            d = g5k.Deployment([self.server], env_file=self.args.server_env)
        else:
            d = g5k.Deployment([self.server], env_name=self.args.server_env,
                           user=self.args.kadeploy_user)
        logger.debug("Deploying environment '{}' on server {}...".format(self.args.server_env,
                                                                         self.server.address))
        deployed, undeployed = g5k.kadeploy.deploy(d)
        if len(deployed) == 0:
            logger.error("Could not deploy server")
            sys.exit(1)

    def prepare_server(self):
        # Server is already deployed
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

# Update git repository for unbound.
cd /root/unbound || rc=$?
git pull || rc=$?
make -j8 || rc=$?

# Install CPUNetLog
apt-get --yes install python3 python3-psutil python3-netifaces
cd /root/
[ -d "CPUnetLOG" ] || git clone https://github.com/jonglezb/CPUnetLOG || rc=$?
cd CPUnetLOG || rc=$?
git pull || rc=$?
exit $rc
        """.format(vm_subnet=self.subnet)
        task = execo.Remote(script, [self.server],
                            connection_params=self.server_conn_params,
                            name="Setup server").start()
        return task

    def start_cpunetlog(self, hosts, conn_params=None):
        script = """/root/CPUnetLOG/__init__.py --stdout"""
        if conn_params == None:
            task = execo.Remote(script, hosts,
                                name="CPUnetLOG").start()
        else:
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
iptables -t raw -A OUTPUT -p tcp -j NOTRACK || rc=$?

# Install CPUNetLog
cd /root/
apt-get --yes install python3 python3-psutil python3-netifaces || rc=$?
[ -d "CPUnetLOG" ] || git clone https://github.com/jonglezb/CPUnetLOG || rc=$?
cd CPUnetLOG || rc=$?
git pull || rc=$?

exit $rc
        """.format(server_name=self.server.address)
        task = execo.Remote(script, self.vm, name="Setup VM").start()
        return task

    def start_dns_server(self):
        unbound_params = {
            "buffer_size": 4096,
            "nb_threads": self.args.server_threads,
            # Add a 5% margin to the number of client slots
            "max_tcp_clients_per_thread": self.args.unbound_slots_per_thread,
        }
        max_clients = self.args.server_threads * self.args.unbound_slots_per_thread
        logger.debug("Unbound using {nb_threads} threads, {max_tcp_clients_per_thread} max clients per thread, {buffer_size}b buffer size".format(**unbound_params))
        logger.debug("Max clients: {}".format(max_clients))
        unbound_config = """\
cat > /tmp/unbound.conf <<EOF
server:
  interface: 0.0.0.0
  access-control: 0.0.0.0/0 allow
  username: root
  use-syslog: no
  chroot: ""
  directory: "."
  pidfile: "/tmp/unbound.pid"
  incoming-num-tcp: {max_tcp_clients_per_thread}
  num-threads: {nb_threads}
  msg-buffer-size: {buffer_size}
  so-reuseport: yes
  local-zone: example.com static
  local-data: "example.com A 42.42.42.42"
EOF
        """.format(**unbound_params)
        execo.Remote(unbound_config, [self.server],
                     connection_params=self.server_conn_params,
                     name="Configure unbound").run()
        # Kill possibly lingering unbound instances
        task = execo.Remote("pkill unbound; sleep 3; /root/unbound/unbound -d -v -c /tmp/unbound.conf",
                            [self.server],
                            connection_params=self.server_conn_params,
                            name="Unbound server process").start()
        return task

    def start_tcpclient_vm(self):
        """Start tcpclient on all VM"""
        # Create a different random seed for each tcpclient, but
        # deterministically based on the global seed.
        random_seed = [self.args.random_seed + vm_id for vm_id, vm in enumerate(self.vm)]
        script = "/root/tcpscaler/tcpclient -s {{{{random_seed}}}} -t {} -R -p 53 -r {} -c {} -n {} {}"
        script = script.format(self.args.client_duration,
                               self.args.client_query_rate,
                               self.args.client_connections,
                               self.args.client_connection_rate,
                               self.server.address)
        task = execo.Remote(script, self.vm, name="tcpclient").start()
        return task

    def log_experimental_conditions(self):
        logger.info("Random seed: {}".format(self.args.random_seed))
        logger.info("Subnet [job {}]: {}".format(self.subnet_job[0],
                                                 self.subnet))
        vmhosts = [s.address for s in g5k.get_oar_job_nodes(*self.vmhosts_job)]
        logger.info("{} VM hosts [job {}]: {}".format(len(vmhosts),
                                                      self.vmhosts_job[0],
                                                      ' '.join(vmhosts)))
        servers = [s.address for s in g5k.get_oar_job_nodes(*self.server_job)]
        logger.info("{} servers [job {}]: {}".format(len(servers),
                                                     self.server_job[0],
                                                     ' '.join(servers)))

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
        unbound = None
        try:
            self.reserve_vmhosts()
            logger.debug("Waiting for VM hosts job to start...")
            g5k.wait_oar_job_start(*self.vmhosts_job)
            self.reserve_server()
            logger.debug("Waiting for server job to start...")
            g5k.wait_oar_job_start(*self.server_job)
            self.reserve_subnet()
            g5k.wait_oar_job_start(*self.subnet_job)
            self.prepare_subnet()
            logger.debug("Prepared subnet")
            self.log_experimental_conditions()
            logger.debug("Setting up VM hosts...")
            machines_setup_process = self.prepare_vmhosts()
            logger.debug("Deploying server image...")
            server_deploy_process = self.deploy_server()
            logger.debug("Server is deployed.")
            machines_setup_process.wait()
            logger.debug("VM hosts are setup.")
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
                unbound = self.start_dns_server()
                logger.info("Started unbound on {}.".format(self.server.address))
                # Leave time for unbound to start
                if self.args.unbound_slots_per_thread < 1000000:
                    execo.sleep(15)
                else:
                    execo.sleep(60)
                logger.info("Starting tcpclient on all VMs...")
                clients = self.start_tcpclient_vm()
                clients.wait()
                logger.info("tcpclient finished!")
                logger.info("Writing cpunetlog output to disk.")
                cpunetlog_server.kill(sig=2).wait()
                cpunetlog_vms.kill(sig=2).wait()
                self.log_output(cpunetlog_server, "cpunetlog_server")
                self.log_output(cpunetlog_vms, "cpunetlog_vms")
                logger.info("writing tcpclient results to disk.")
                self.log_output(clients, "clients", log_stdout=False)
                with open(rtt_file, 'w') as rtt_output:
                    need_header = True
                    rtt = csv.writer(rtt_output)
                    for client_id, client in enumerate(clients.processes):
                        first_line = True
                        for line in iter(client.stdout.splitlines()):
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
            if unbound:
                unbound.kill()
                logger.debug("Waiting for unbound to exit")
                unbound.wait()
                self.log_output(unbound, "unbound")
            if self.vm_process:
                logger.debug("Waiting for VM to exit")
                self.vm_process.wait()
                logger.info("Unbound an all VMs are shut down")
                self.log_output(self.vm_process, "vm_process")
                print(execo.Report([self.vm_process]).to_string())
            #for s in self.vm_process.processes:
            #    print("\n%s\nstdout:\n%s\nstderr:\n%s\n" % (s, s.stdout, s.stderr))
            #g5k.oardel([job, subnet_job])


if __name__ == "__main__":
    engine = DNSServerExperiment()
    engine.start()
