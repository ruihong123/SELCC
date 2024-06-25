#!/bin/bash
set -o nounset
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..
#BIN_HOME=$bin/../release
# With the specified arguments for benchmark setting,
# this script_compute runs tpcc for varied distributed ratios

# specify your hosts_file here
# hosts_file specify a list of host names and port numbers, with the host names in the first column
#Compute_file="../tpcc/compute.txt"
#Memory_file="../tpcc/memory.txt"
conf_file_all=$bin/../connection_cloudlab.conf
conf_file="../connection.conf"

#awk -v pos="$node" -F' ' '{
#        for (i=1; i<=NF; i++) {
#            if (i <= pos) {
#                printf("%s", $i)
#                if (i < pos) printf(" ")
#            }
#        }
#        print ""
#    }' "$conf_file_all" > "$conf_file"

# specify your directory for log files
output_dir="/users/Ruihong/MemoryEngine/scripts/data"
core_dump_dir="/mnt/core_dump"
# working environment
proj_dir="/users/Ruihong/MemoryEngine"
bin_dir="${proj_dir}/debug"
script_dir="${proj_dir}/database/scripts"
ssh_opts="-o StrictHostKeyChecking=no"

compute_line=$(sed -n '1p' $conf_file)
memory_line=$(sed -n '2p' $conf_file)
read -r -a compute_nodes <<< "$compute_line"
read -r -a memory_nodes <<< "$memory_line"
compute_num=${#compute_nodes[@]}
memory_num=${#memory_nodes[@]}

#compute_nodes=(`echo ${compute_list}`)
#memory_nodes=(`echo ${memory_list}`)
master_host=${compute_nodes[0]}
cache_mem_size=8 # 8 gb Local memory size (Currently not working)
remote_mem_size=55 # 8 gb Remote memory size pernode is enough
port=$((13000+RANDOM%1000))

compute_ARGS="$@"

echo "input Arguments: ${compute_ARGS}"
echo "launch..."

launch () {

  read -r -a memcached_node <<< $(head -n 1 $proj_dir/memcached_ip.conf)
  echo "restart memcached on ${memcached_node[0]}"
  ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"
  rm /proj/purduedb-PG0/logs/core

  dist_ratio=$1
  echo "start tpcc for dist_ratio ${dist_ratio}"
  output_file="${output_dir}/${dist_ratio}_tpcc.log"
  memory_file="${output_dir}/Memory.log"
  for ((i=0;i<${#memory_nodes[@]};i++)); do
        memory=${memory_nodes[$i]}
        script_memory="cd ${bin_dir} && ./memory_server_tpcc $port $(($remote_mem_size)) $((2*$i +1)) > ${output_file} 2>&1"
        echo "start worker: ssh ${ssh_opts} ${memory} '$script_memory' &"
        ssh ${ssh_opts} ${memory} "echo '$core_dump_dir/core$memory' | sudo tee /proc/sys/kernel/core_pattern"
        ssh ${ssh_opts} ${memory} " $script_memory" &
        sleep 1
  done
  script_compute="cd ${bin_dir} && ./tpcc ${compute_ARGS} -d${dist_ratio}"
  echo "start master: ssh ${ssh_opts} ${master_host} '$script_compute -sn$master_host  -nid0 | tee -a ${output_file} "
  ssh ${ssh_opts} ${master_host} "echo '$core_dump_dir/core$master_host' | sudo tee /proc/sys/kernel/core_pattern"

  ssh ${ssh_opts} ${master_host} "ulimit -S -c unlimited && $script_compute -sn$master_host -nid0 |tee -a ${output_file}" &
#  sleep 1

  for ((i=1;i<${#compute_nodes[@]};i++)); do
    compute=${compute_nodes[$i]}
    echo "start worker: ssh ${ssh_opts} ${compute} '$script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}' &"
    ssh ${ssh_opts} ${compute} "echo '$core_dump_dir/core$compute' | sudo tee /proc/sys/kernel/core_pattern"
    ssh ${ssh_opts} ${compute} "ulimit -S -c unlimited && $script_compute -sn$compute -nid$((2*$i)) | tee -a ${output_file}" &
#    sleep 1
  done

  wait
  echo "done for ${dist_ratio}"
}

run_tpcc () {
  dist_ratios=(0)
  for dist_ratio in ${dist_ratios[@]}; do
    launch ${dist_ratio}
  done
}

vary_read_ratios () {
  #read_ratios=(0 30 50 70 90 100)
  read_ratios=(0)
  for read_ratio in ${read_ratios[@]}; do
    old_user_args=${compute_ARGS}
    compute_ARGS="${compute_ARGS} -r${read_ratio}"
    run_tpcc
    compute_ARGS=${old_user_args}
  done
}

vary_thread_number () {
  #read_ratios=(0 30 50 70 90 100)
  thread_number=(8)
  for thread_n in ${thread_number[@]}; do
    compute_ARGS="-p$port -sf64 -sf1 -c$thread_n -t1000000 -f../connection.conf"
    run_tpcc
  done
}

vary_temp_locality () {
  #localities=(0 30 50 70 90 100)
  localities=(0 50 100)
  for locality in ${localities[@]}; do
    old_user_args=${compute_ARGS}
    compute_ARGS="${compute_ARGS -l${locality}}"
    run_tpcc
    compute_ARGS=${old_user_args}
  done
}

auto_fill_params () {
  # so that users don't need to specify parameters for themselves
  compute_ARGS="-p$port -sf512 -sf1 -c4 -t200000 -f../connection.conf"
}

auto_fill_params
# run standard tpcc
#run_tpcc
vary_thread_number
# vary_read_ratios
#vary_temp_locality
