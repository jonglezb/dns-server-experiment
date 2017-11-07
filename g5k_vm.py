#!/usr/bin/env python3

import argparse

import execo
import execo_g5k as g5k


def reserve_machines(cluster, nb_machines):
    if cluster:
        submission = g5k.OarSubmission(resources="{{cluster='{}'}}/switch=1/nodes={}".format(cluster, nb_machines))
    else:
        submission = g5k.OarSubmission(resources="switch=1/nodes={}".format(nb_machines))
    [(jobid, site)] = g5k.oarsub([(submission , None)])
    return (jobid, site)

def prepare_machines(job, nb_vm):
    g5k.wait_oar_job_start(*job)
    nodes = g5k.get_oar_job_nodes(*job)
    execo.Remote("sudo-g5k iptables -t raw -A PREROUTING -p tcp -j NOTRACK; sudo-g5k iptables -t raw -A OUTPUT -p tcp -j NOTRACK",
                 nodes, connection_params=g5k.default_oarsh_oarcp_params).run()

def start_all_vm(job, nb_vm, memory):
    """Starts VM on all reserved machines, and returns the associated task
    object.  This function will return immediately, but the caller has to
    wait for the VM to be setup before using them.
    """
    nodes = g5k.get_oar_job_nodes(*job)
    create_all_vm = """\
for i in $(seq 0 {vm_max})
do
  iface="tap$i"
  ip link show dev $iface > /dev/null || sudo-g5k create_tap
  sudo-g5k kvm -m {memory} -smp cores={cores},threads=1,sockets=1 -nographic -localtime -enable-kvm -drive file="$image",if=virtio,media=disk -net nic,model=virtio,macaddr="$mac" -net tap,ifname=$iface,script=no &
done
wait
    """.format(vm_max=nb_vm-1, memory=memory)
    return execo.Remote(create_all_vm, nodes, connection_params=g5k.default_oarsh_oarcp_params).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Deploy VMs on grid5000, to perform experiments')
    parser.add_argument('--cluster', '-c',
                        help='Which Grid5000 cluster to use (defaut: any cluster)')
    parser.add_argument('--nb-hosts', '-N', type=int, default=1,
                        help='Number of physical machines to reserve on the cluster')
    parser.add_argument('--job-id', '-j', type=int,
                        help='Instead of making a reservation, use an existing OAR job ID')
    parser.add_argument('--nb-vm', '-n', type=int, default=1,
                        help='Number of VM to spawn on each physical machine')
    parser.add_argument('--memory', '-m', type=int, default=2048,
                        help='Memory in MB to allocate to each VM')
    args = parser.parse_args()
    if args.job_id:
        job = (args.job_id, None)
    else:
        job = reserve_machines(args.cluster, args.nb_hosts)
    if job[0]:
        try:
            prepare_machines(job, args.nb_vm)
            start_all_vm(job, args.nb_vm, args.memory)
        finally:
            #g5k.oardel([job])
            pass

