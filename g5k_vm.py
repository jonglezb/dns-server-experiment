#!/usr/bin/env python3

import argparse

import execo
import execo_g5k as g5k


def reserve_subnet(args):
    submission = g5k.OarSubmission(resources="slash_22=1", walltime=args.walltime)
    [(jobid, site)] = g5k.oarsub([(submission , None)])
    return (jobid, site)

def reserve_machines(args):
    if args.cluster:
        submission = g5k.OarSubmission(resources="{{cluster='{}'}}/switch=1/nodes={}".format(args.cluster, args.nb_hosts),
                                       walltime=args.walltime)
    else:
        submission = g5k.OarSubmission(resources="switch=1/nodes={}".format(args.nb_hosts),
                                       walltime=args.walltime)
    [(jobid, site)] = g5k.oarsub([(submission , None)])
    return (jobid, site)

def prepare_machines(job, nb_vm):
    g5k.wait_oar_job_start(*job)
    nodes = g5k.get_oar_job_nodes(*job)
    execo.Remote("sudo-g5k iptables -t raw -A PREROUTING -p tcp -j NOTRACK; sudo-g5k iptables -t raw -A OUTPUT -p tcp -j NOTRACK",
                 nodes, connection_params=g5k.default_oarsh_oarcp_params).run()

def start_all_vm(job, args, ip_mac_list):
    """Starts VM on all reserved machines, and returns the associated task
    object.  This function will return immediately, but the caller has to
    wait for the VM to be setup before using them.
    """
    nb_vm = args.nb_vm
    memory = args.memory
    nodes = g5k.get_oar_job_nodes(*job)
    [ips, macs] = zip(*ip_mac_list)
    # For each physical host, build a list of MAC addresses to be used for its VMs
    macs_per_host = [macs[i*nb_vm:(i+1)*nb_vm] for i, host in enumerate(nodes)]
    vm_ips = ips[:len(nodes)*args.nb_vm]
    print("VMs IP: {}".format(' '.join(vm_ips)))
    # Double escaping is magic (after .format, it will become {{macs_per_host}})
    script = """\
for mac in {{{{[' '.join(macs) for macs in macs_per_host]}}}}
do
  iface=$(sudo-g5k create_tap)
  kvm -m {memory} -smp cores={cores},threads=1,sockets=1 -nographic -localtime -enable-kvm -drive file="{image}",if=virtio,media=disk -net nic,model=virtio,macaddr="$mac" -net tap,ifname="$iface",script=no &
done
wait
    """.format(memory=memory, cores=1, image="todo")
    vm_task = execo.Remote(script, nodes, connection_params=g5k.default_oarsh_oarcp_params, name="VM task")
    return vm_task.start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deploy VMs on grid5000, to perform experiments')
    parser.add_argument('--cluster', '-c',
                        help='Which Grid5000 cluster to use (defaut: any cluster)')
    parser.add_argument('--nb-hosts', '-N', type=int, default=1,
                        help='Number of physical machines to reserve on the cluster (default: %(default)s)')
    parser.add_argument('--job-id', '-j', type=int,
                        help='Instead of making a reservation for machines, use an existing OAR job ID')
    parser.add_argument('--subnet-job-id', '-J', type=int,
                        help='Instead of making a reservation for a subnet, use an existing OAR job ID')
    parser.add_argument('--nb-vm', '-n', type=int, default=1,
                        help='Number of VM to spawn on each physical machine (default: %(default)s)')
    parser.add_argument('--memory', '-m', type=int, default=2048,
                        help='Memory in MB to allocate to each VM (default: %(default)s)')
    parser.add_argument('--walltime', '-t', type=int,
                        help='How much time the reservations should last, in seconds')
    args = parser.parse_args()
    if args.subnet_job_id:
        subnet_job = (args.subnet_job_id, None)
    else:
        subnet_job = reserve_subnet(args)
    if not subnet_job[0]:
        print("Can't reserve subnet", file=sys.stderr)
        exit(1)
    (ip_mac_list, subnet_params) = g5k.get_oar_job_subnets(*subnet_job)
    vm_subnet = subnet_params['ip_prefix'] # Use this to add a route on the server
    print("Subnet reserved: prefix is {}".format(vm_subnet))
    if args.job_id:
        job = (args.job_id, None)
    else:
        job = reserve_machines(args)
    if job[0]:
        try:
            prepare_machines(job, args.nb_vm)
            task = start_all_vm(job, args, ip_mac_list)
            print("Started all VMs, waiting for them to terminate.")
            task.wait()
            print(execo.Report([task]).to_string())
            for s in task.processes:
                print("\n%s\nstdout:\n%s\nstderr:\n%s\n" % (s, s.stdout, s.stderr))
        finally:
            #g5k.oardel([job, subnet_job])
            pass
