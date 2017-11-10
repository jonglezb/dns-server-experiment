#!/usr/bin/env python3

import argparse

import execo
import execo_g5k as g5k
import execo_engine as engine
from execo_engine import logger

class DNSServerExperiment(engine.Engine):
    def __init__(self):
        super(DNSServerExperiment, self).__init__()
        ## Parse command-line arguments
        self.args_parser.add_argument('--cluster',
                            help='Which Grid5000 cluster to use (defaut: any cluster)')
        self.args_parser.add_argument('--nb-hosts', '-N', type=int, default=2,
                            help='Number of physical machines to reserve on the cluster (default: %(default)s)')
        self.args_parser.add_argument('--job-id', '-j', type=int,
                            help='Instead of making a reservation for machines, use an existing OAR job ID')
        self.args_parser.add_argument('--subnet-job-id', '-J', type=int,
                            help='Instead of making a reservation for a subnet, use an existing OAR job ID')
        self.args_parser.add_argument('--vm-image', '-i', required=True,
                            help='Path to the qcow2 VM image to use (on the G5K frontend)')
        self.args_parser.add_argument('--nb-vm', '-n', type=int, default=1,
                            help='Number of VM to spawn on each physical machine (default: %(default)s)')
        self.args_parser.add_argument('--memory', '-m', type=int, default=2048,
                            help='Memory in MB to allocate to each VM (default: %(default)s)')
        self.args_parser.add_argument('--walltime', '-t', type=int,
                            help='How much time the reservations should last, in seconds')

    def init(self):
        ## Physical machines
        # OAR job for machines, represented as (oarjob ID, frontend)
        self.machines_job = None
        # Machine (execo.host.Host) to be used as server in the experiment
        self.server = None
        # List of machines (execo.host.Host) to be used to host VMs
        self.vm_hosts = []
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

    def reserve_machines(self):
        # Existing job
        if self.args.job_id:
            self.machines_job = (self.args.job_id, None)
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
        self.machines_job = (jobid, site)

    def prepare_subnet(self):
        # subnet_params is a dict: http://execo.gforge.inria.fr/doc/latest-stable/execo_g5k.html#get-oar-job-subnets
        (ip_mac_list, subnet_params) = g5k.get_oar_job_subnets(*self.subnet_job)
        self.subnet = subnet_params['ip_prefix']
        self.subnet_ip_mac = ip_mac_list

    def prepare_machines(self):
        nodes = g5k.get_oar_job_nodes(*self.machines_job)
        # Split machines into one server, and several VM hosts
        self.server = nodes[0]
        self.vm_hosts = nodes[1:]
        # Avoid conntrack on all machines
        task = execo.Remote("sudo-g5k iptables -t raw -A PREROUTING -p tcp -j NOTRACK; sudo-g5k iptables -t raw -A OUTPUT -p tcp -j NOTRACK",
                            nodes, connection_params=g5k.default_oarsh_oarcp_params).start()
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

    def prepare_server(self):
        script = """\
# Add direct route to VM network
sudo-g5k ip route replace {vm_subnet} dev br0 || exit 1

# Increase max number of incoming connections
sudo-g5k sysctl net.ipv4.tcp_syncookies=0 || exit 1
sudo-g5k sysctl net.core.somaxconn=8192 || exit 1
sudo-g5k sysctl fs.file-max=12582912 || exit 1
        """.format(vm_subnet=self.subnet)
        task = execo.Remote(script, [self.server], connection_params=g5k.default_oarsh_oarcp_params, name="Setup server").start()
        return task

    def wait_until_vm_ready(self):
        self.vm = [execo.Host(ip, user='root') for ip in self.vm_ips]
        # TODO: find a better way
        execo.sleep(5)
        return
        task = execo.Remote("date", self.vm, name="Test VM reachability").run()
        print(execo.Report([task]).to_string())

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

    def run(self):
        try:
            self.reserve_subnet()
            self.reserve_machines()
            g5k.wait_oar_job_start(*self.subnet_job)
            self.prepare_subnet()
            logger.debug("Prepared subnet")
            g5k.wait_oar_job_start(*self.machines_job)
            machines_setup_process = self.prepare_machines()
            machines_setup_process.wait()
            logger.debug("Prepared physical machines")
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
                logger.info("Started all VMs, waiting for them to terminate.")
                self.vm_process.wait()
        finally:
            print(execo.Report([self.vm_process]).to_string())
            for s in self.vm_process.processes:
                print("\n%s\nstdout:\n%s\nstderr:\n%s\n" % (s, s.stdout, s.stderr))
            #g5k.oardel([job, subnet_job])
            pass


if __name__ == "__main__":
    engine = DNSServerExperiment()
    engine.start()
