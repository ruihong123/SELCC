#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/..
BIN_HOME=$bin/../release
conf_file_all=$bin/../connection_cloudlab.conf
conf_file=$bin/../connection.conf
# alpha = 1/(1-theta)
#compute_nodes=$bin/compute_nodes
#memory_nodes=$bin/memory_nodes
log_file=$bin/log
cache_mem_size=8 # 8 gb Local memory size
remote_mem_size_base=48 # 48 gb Remote memory size totally
#master_ip=db3.cs.purdue.edu # make sure this is in accordance with the server whose is_master=1
master_port=12311
port=$((10000+RANDOM%1000))
#compute_num = 0
#memory_num = 0
run() {
    echo "run for result_file=$result_file,
    thread=$thread, zipfian_alpha=$zipfian_alpha, workload=$workload,
    remote_ratio=$remote_ratio, shared_ratio=$shared_ratio,
    read_ratio=$read_ratio, op_type=$op_type,
    space_locality=$space_locality, time_locality=$time_locality"

#    compute_line_all=$(sed -n '1p' conf_file_all)
#    memory_line_all=$(sed -n '2p' conf_file_all)
    awk -v pos="$node" -F' ' '{
        for (i=1; i<=NF; i++) {
            if (i <= pos) {
                printf("%s", $i)
                if (i < pos) printf(" ")
            }
        }
        print ""
    }' "$conf_file_all" > "$conf_file"

    old_IFS=$IFS
    IFS=' '
    compute_line=$(sed -n '1p' $conf_file)
    memory_line=$(sed -n '2p' $conf_file)
    read -r -a compute_nodes <<< "$compute_line"
    read -r -a memory_nodes <<< "$memory_line"
    compute_num=${#compute_nodes[@]}
    memory_num=${#memory_nodes[@]}
    echo "memory nodes:"
    i=0
    for memory in "${memory_nodes[@]}"
    do
       echo $memory
       i=$((i+1))
    done
    i=0
    echo "compute nodes:"
    for compute in "${compute_nodes[@]}"
    do
       echo $compute
       i=$((i+1))
    done

    j=0
    for compute in "${compute_nodes[@]}"
    do
      ip=`echo $compute | cut -d ' ' -f1`
      ssh -o StrictHostKeyChecking=no $ip "killall micro_bench > /dev/null 2>&1 && cd $BIN_HOME"
      j=$((j+1))
  #		if [ $j = $node ]; then
  #			break;
  #		fi
    done

    j=0
      for memory in "${memory_nodes[@]}"
      do
        ip=`echo $memory | cut -d ' ' -f1`
        ssh -o StrictHostKeyChecking=no $ip "killall memory_server_term > /dev/null 2>&1 && cd $BIN_HOME"
        j=$((j+1))
  #  		if [ $j = $node ]; then
  #  			break;
  #  		fi
      done

    i=0
    while [ $i -lt $memory_num ]
    do
      echo "Rsync the connection.conf to ${memory_nodes[$i]}"
      rsync -vz /users/Ruihong/SELCC/connection.conf ${memory_nodes[$i]}:/users/Ruihong/SELCC/connection.conf
      i=$((i+1))
    done
    i=0
    while [ $i -lt $compute_num ]
    do
      echo "Rsync the connection.conf to ${compute_nodes[$i]}"
      rsync -vz /users/Ruihong/SELCC/connection.conf ${compute_nodes[$i]}:/users/Ruihong/SELCC/connection.conf
      i=$((i+1))
    done
    i=0
#    compute_nodes_arr=`cat "$compute_nodes"`
#    memory_nodes_arr=`cat "$memory_nodes"`
#    echo ${#memory_nodes[@]}
#    echo ${#memory_nodes[@]}

#    compute_num=$(wc -l < $compute_nodes)
#    memory_num=$(wc -l < $memory_nodes)
#    compute_num=$((compute_num+1))
#    memory_num=$((memory_num+1))
#    echo `cat $slaves`
    echo $compute_num
    echo $memory_num
  	read -r -a memcached_node <<< $(head -n 1 $SRC_HOME/memcached_ip.conf)
  	echo "restart memcached on ${memcached_node[0]}"
    ssh -o StrictHostKeyChecking=no ${memcached_node[0]} "sudo service memcached restart"

    if [ $size_grow = 1  ]; then
      remote_mem_size=$(($remote_mem_size_base*$compute_num))
    else
      remote_mem_size=$remote_mem_size_base
    fi

    for memory in "${memory_nodes[@]}"
        do
          ip=$memory
#            	port=`echo $memory | cut -d ' ' -f2`
          echo ""
          echo "memory = $memory, ip = $ip, port = $port"
          echo "$BIN_HOME/memory_server_term  $port $(($remote_mem_size+10)) $((2*$i +1)) $remote_mem_size | tee -a $log_file.$ip"
          ssh -o StrictHostKeyChecking=no $ip	"ulimit -c 500 && cd $BIN_HOME && numactl --physcpubind=31 ./memory_server_term  $port $(($remote_mem_size+10)) $((2*$i +1)) $remote_mem_size | tee -a $log_file.$ip " &
          sleep 1
          i=$((i+1))
#        	if [ "$i" = "$node" ]; then
#        		break
#        	fi
        done # for slave
    sleep 2
    i=0
    for compute in "${compute_nodes[@]}"
      do
        ip=$compute
        if [ $i = 0 ]; then
          is_master=1
              master_ip=$ip
        else
          is_master=0
        fi

        if [ $single_writer = 1  ]; then
            if [ $i = 0 ]; then
              read_ratio="0"
            else
              read_ratio="100"
            fi
        fi
#        if [ $port == $ip ]; then
#          port=12345
#        fi
        echo ""
        echo "compute = $compute, ip = $ip, port = $port"
        echo "$BIN_HOME/micro_bench --op_type $op_type --workload $workload --zipfian_alpha $zipfian_alpha --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file $result_file --this_node_id $((2*$i)) --tcp_port $port --is_master $is_master --cache_size $cache_mem_size --allocated_mem_size $remote_mem_size --compute_num $compute_num --memory_num $memory_num | tee -a $log_file.$ip"
        ssh -o StrictHostKeyChecking=no $ip	"ulimit -c 50000000 && cd $BIN_HOME && ./micro_bench --op_type $op_type --workload $workload --zipfian_alpha $zipfian_alpha --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file $result_file --this_node_id $((2*$i)) --tcp_port $port --is_master $is_master --cache_size $cache_mem_size --allocated_mem_size $remote_mem_size --compute_num $compute_num --memory_num $memory_num | tee -a $log_file.$ip" &
        sleep 1
        i=$((i+1))
  #    	if [ "$i" = "$node" ]; then
  #    		break
  #    	fi
      done # for compute

	  wait


    sleep 1
    IFS="$old_IFS"
}

run_test() {
# node test
echo "**************************run node test****************************"
result_file=$bin/results/node
node_range="1 2 4 8"
thread_range="8"
remote_range="100"
shared_range="100"
size_grow=0 # 0 not grow, 1 grow with node number
single_writer=0
read_range="0"
space_range="0"
time_range="0"
workload_range="1" # 0 uniform, 1 single zipfian, n >1 multispot zipfian. -1 accesss only single cache line
zipfian_alpha_range="0.99 10" #make sure workload = 1 if we want to test zipfian.
#
op_range="1" # use 1, 2 for operation with spinning
#cache_th=0.5
for workload in $workload_range
do
for zipfian_alpha in $zipfian_alpha_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
  if [ $shared_ratio != 100 && $size_grow=1]; then
      exit
  fi
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do

for thread in $thread_range
do
for node in $node_range
do
  echo $node
#    remote_ratio=`echo "($node-1)*100/$node" | bc`
#    echo $remote_ratio
#    if [[ $node = 1 ]]; then
#        continue;
#    fi
  run
done
done
done
done
done
done
done
done
done
done
}


run_node_test() {
# node test
echo "**************************run node test****************************"
result_file=$bin/results/node
node_range="1 2 4 8 16"
thread_range="8"
remote_range="100"
shared_range="0 30 60 100"
size_grow=0 # 0 not grow, 1 grow with node number
read_range="0 50 95"
space_range="0"
time_range="0"
workload_range="0" # 0 uniform, 1 single zipfian, n >1 multispot zipfian.
zipfian_alpha_range="0.99" #make sure workload = 1 if we want to test zipfian.
#
op_range="1" # use 1
#cache_th=0.5
for workload in $workload_range
do
for zipfian_alpha in $zipfian_alpha_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
  if [ $shared_ratio != 100 && $size_grow=1]; then
      exit
  fi
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do

for thread in $thread_range
do
for node in $node_range
do
  echo $node
#    remote_ratio=`echo "($node-1)*100/$node" | bc`
#    echo $remote_ratio
#    if [[ $node = 1 ]]; then
#        continue;
#    fi
  run
done
done
done
done
done
done
done
done
done
done
}

run_skewed_test() {
# node test
echo "**************************run node test****************************"
result_file=$bin/results/node
node_range="8"
thread_range="8"
remote_range="100"
shared_range="100"
size_grow=0 # 0 not grow, 1 grow with node number
single_writer=0
read_range="0"
space_range="0"
time_range="0"
workload_range="1" # 0 uniform, 1 single zipfian, n >1 multispot zipfian. -1 accesss only single cache line
zipfian_alpha_range="10" #make sure workload = 1 if we want to test zipfian.
#
op_range="2" # use 1, 2 is for operation with spinning
#cache_th=0.5
for workload in $workload_range
do
for zipfian_alpha in $zipfian_alpha_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
  if [ $shared_ratio != 100 && $size_grow=1]; then
      exit
  fi
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do

for thread in $thread_range
do
for node in $node_range
do
  echo $node
#    remote_ratio=`echo "($node-1)*100/$node" | bc`
#    echo $remote_ratio
#    if [[ $node = 1 ]]; then
#        continue;
#    fi
  run
done
done
done
done
done
done
done
done
done
done
}





#run_read_test
#run_time_test
#run_shared_test
#run_remote_test
#run_space_test
#run_shared_test_noeviction
#run_node_test
run_test
#run_skewed_test
