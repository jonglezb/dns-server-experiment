#!/usr/bin/env python3

import argparse
import time
import os
import sys

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
        self.args_parser.add_argument('--vm-image', '-i', required=True,
                            help='Path to the qcow2 VM image to use (on the G5K frontend)')
        self.args_parser.add_argument('--nb-vm', '-n', type=int, default=1,
                            help='Number of VM to spawn on each VM host (default: %(default)s)')
        self.args_parser.add_argument('--memory', '-m', type=int, default=2048,
                            help='Memory in MB to allocate to each VM (default: %(default)s)')
        self.args_parser.add_argument('--walltime', '-t', type=int,
                            help='How much time the reservations should last, in seconds.  Unused if -j, -J and -S are passed.')

    def init(self):
        ## Physical machines
        # OAR job for VM hosts, represented as (oarjob ID, frontend)
        self.vmhosts_job = None
        # List of machines (execo.host.Host) to be used to host VMs
        self.vm_hosts = []
        # OAR job for the server, represented as (oarjob ID, frontend)
        self.server_job = None
        # Machine (execo.host.Host) to be used as server in the experiment
        self.server = None
        self.server_conn_params = {'user': 'root'}
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
        submission = g5k.OarSubmission(resources="slash_22=1",
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission , None)])
        self.subnet_job = (jobid, site)

    def reserve_vmhosts(self):
        # Existing job
        if self.args.vmhosts_job_id:
            self.vmhosts_job = (self.args.vmhosts_job_id, None)
            return
        # New job
        if self.args.cluster:
            resources = "{{cluster='{}'}}/switch=1/nodes={}".format(self.args.cluster,
                                                                    self.args.nb_hosts)
        else:
            resources = "switch=1/nodes={}".format(self.args.nb_hosts)
        submission = g5k.OarSubmission(resources=resources,
                                       walltime=self.args.walltime)
        [(jobid, site)] = g5k.oarsub([(submission , None)])
        self.vmhosts_job = (jobid, site)

    def reserve_server(self):
        # Existing job
        if self.args.server_job_id:
            self.server_job = (self.args.server_job_id, None)
            return
        # New job
        if self.args.cluster:
            resources = "{{cluster='{}'}}/switch=1/nodes=1".format(self.args.cluster)
        else:
            resources = "switch=1/nodes=1"
        submission = g5k.OarSubmission(resources=resources,
                                       walltime=self.args.walltime,
                                       job_type="deploy")
        [(jobid, site)] = g5k.oarsub([(submission , None)])
        self.server_job = (jobid, site)

    def prepare_subnet(self):
        # subnet_params is a dict: http://execo.gforge.inria.fr/doc/latest-stable/execo_g5k.html#get-oar-job-subnets
        (ip_mac_list, subnet_params) = g5k.get_oar_job_subnets(*self.subnet_job)
        self.subnet = subnet_params['ip_prefix']
        self.subnet_ip_mac = ip_mac_list

    def prepare_vmhosts(self):
        self.vm_hosts = g5k.get_oar_job_nodes(*self.vmhosts_job)
        # Avoid conntrack on all machines
        task = execo.Remote("sudo-g5k iptables -t raw -A PREROUTING -p tcp -j NOTRACK; sudo-g5k iptables -t raw -A OUTPUT -p tcp -j NOTRACK",
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
        self.server = g5k.get_oar_job_nodes(*self.server_job)[0]
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
# Add direct route to VM network
ip route replace {vm_subnet} dev eth0 || exit 1

# Increase max number of incoming connections
sysctl net.ipv4.tcp_syncookies=0 || exit 1
sysctl net.core.somaxconn=8192 || exit 1
sysctl fs.file-max=12582912 || exit 1
echo 12582912 > /proc/sys/fs/nr_open || exit 1
        """.format(vm_subnet=self.subnet)
        task = execo.Remote(script, [self.server],
                            connection_params=self.server_conn_params,
                            name="Setup server").start()
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
# Add direct route to server.
# We use the old-style "route" because it can resolve DNS names, unlike "ip route"
route add {server_name} eth0 || exit 1

# Increase max number of outgoing connections
sysctl net.ipv4.ip_local_port_range="1024 65535" || exit 1
sysctl net.ipv4.tcp_tw_reuse=1 || exit 1

# No connection tracking
iptables -t raw -A PREROUTING -p tcp -j NOTRACK || exit 1
iptables -t raw -A OUTPUT -p tcp -j NOTRACK || exit 1
        """.format(server_name=self.server.address)
        task = execo.Remote(script, self.vm, name="Setup VM").start()
        return task

    def start_dns_server(self):
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
  incoming-num-tcp: 350000
  num-threads: 32
  msg-buffer-size: 4096
  so-reuseport: yes
  local-zone: example.com static
  local-data: "example.com A 42.42.42.42"
EOF
        """
        execo.Remote(unbound_config, [self.server],
                     connection_params=self.server_conn_params,
                     name="Configure unbound").run()
        task = execo.Remote("/root/unbound/unbound -d -v -c /tmp/unbound.conf",
                            [self.server],
                            connection_params=self.server_conn_params,
                            name="Unbound server process").start()
        return task

    def run(self):
        try:
            self.reserve_subnet()
            self.reserve_vmhosts()
            self.reserve_server()
            g5k.wait_oar_job_start(*self.subnet_job)
            self.prepare_subnet()
            logger.debug("Prepared subnet")
            logger.debug("Waiting for VM hosts job to start...")
            g5k.wait_oar_job_start(*self.vmhosts_job)
            logger.debug("Waiting for server job to start...")
            g5k.wait_oar_job_start(*self.server_job)
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
                logger.debug("Prepared server: {}".format(self.server.address))
                vm_setup_process.wait()
                logger.debug("Prepared VM")
                logger.info("Started {} VMs.".format(len(self.vm)))
                unbound = self.start_dns_server()
                logger.info("Started unbound on {}.".format(self.server.address))
                unbound.wait()
                #self.vm_process.wait()
        finally:
            self.kill_all_vm()
            print(execo.Report([self.vm_process]).to_string())
            for s in self.vm_process.processes:
                print("\n%s\nstdout:\n%s\nstderr:\n%s\n" % (s, s.stdout, s.stderr))
            #g5k.oardel([job, subnet_job])
            pass


if __name__ == "__main__":
    engine = DNSServerExperiment()
    engine.start()
