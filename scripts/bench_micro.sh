#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin/../src
compute_nodes=$bin/compute_nodes
memory_nodes=$bin/memory_nodes
log_file=$bin/log
cache_mem_size=8 # 8 gb Local memory size
remote_mem_size=48 # 48 gb Remote memory size
master_ip=db3.cs.purdue.edu # make sure this is in accordance with the server whose is_master=1
master_port=12311
#compute_num = 0
#memory_num = 0
run() {
    echo "run for result_file=$result_file,
    thread=$thread,
    remote_ratio=$remote_ratio, shared_ratio=$shared_ratio,
    read_ratio=$read_ratio, op_type=$op_type,
    space_locality=$space_locality, time_locality=$time_locality"

    old_IFS=$IFS
    IFS=$'\n'
    i=0
#    compute_nodes_arr=`cat "$compute_nodes"`
#    memory_nodes_arr=`cat "$memory_nodes"`
#    echo $compute_nodes_arr
#    echo $memory_nodes_arr
#    compute_num=${#$compute_nodes_arr[@]}
#    memory_num=${#$memory_nodes_arr[@]}
    compute_num=$(wc -l < $compute_nodes)
    memory_num=$(wc -l < $memory_nodes)
    compute_num=$((compute_num+1))
    memory_num=$((memory_num+1))
#    echo `cat $slaves`
    echo $compute_num
    echo $memory_num
    for compute in `cat "$compute_nodes"`
    do
    	ip=`echo $compute | cut -d ' ' -f1`
    	port=`echo $compute | cut -d ' ' -f2`
    	if [ $i = 0 ]; then
    		is_master=1
            master_ip=$ip
    	else
    		is_master=0
    	fi
    	if [ $port == $ip ]; then
    		port=12345
    	fi
    	echo ""
    	echo "compute = $compute, ip = $ip, port = $port"
    	echo "$SRC_HOME/benchmark --op_type $op_type --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file $result_file --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --cache_size $cache_mem_size --allocated_mem_size $remote_mem_size --compute_num $compute_num --memory_num $memory_num" | tee -a "$log_file".$ip
    	ssh -i ~/.ssh/id_rsa $ip	"$SRC_HOME/benchmark --op_type $op_type --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file "$result_file" --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --cache_size $cache_mem_size --allocated_mem_size $remote_mem_size --compute_num $compute_num --memory_num $memory_num | tee -a '$log_file'.$ip" &
    	sleep 1
    	i=$((i+1))
#    	if [ "$i" = "$node" ]; then
#    		break
#    	fi
    done # for compute
    for memory in `cat "$memory_nodes"`
        do
        	ip=`echo $memory | cut -d ' ' -f1`
        	port=`echo $memory | cut -d ' ' -f2`
        	if [ $i = 0 ]; then
        		is_master=1
                master_ip=$ip
        	else
        		is_master=0
        	fi
        	if [ $port == $ip ]; then
        		port=12345
        	fi
        	echo ""
        	echo "memory = $memory, ip = $ip, port = $port"
        	echo "$SRC_HOME/memory_server --op_type $op_type  --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file $result_file --ip_master $master_ip --ip_worker $ip --port_worker $port --port_master $master_port --cache_size $cache_mem_size --allocated_mem_size $remote_mem_size --compute_num $compute_num --memory_num $memory_num" | tee -a "$log_file".$ip
        	ssh -i ~/.ssh/id_rsa $ip	"$SRC_HOME/memory_server --op_type $op_type --no_thread $thread --shared_ratio $shared_ratio --read_ratio $read_ratio --space_locality $space_locality --time_locality $time_locality --result_file "$result_file" --ip_master $master_ip --ip_worker $ip --port_worker $port --port_master $master_port --cache_size $cache_mem_size --allocated_mem_size $remote_mem_size --compute_num $compute_num --memory_num $memory_num | tee -a '$log_file'.$ip" &
        	sleep 1
        	i=$((i+1))
#        	if [ "$i" = "$node" ]; then
#        		break
#        	fi
        done # for slave
	wait
	j=0
	for compute in `cat $compute_nodes`
	do
		ip=`echo $compute | cut -d ' ' -f1`
		ssh -i ~/.ssh/id_rsa $ip killall benchmark > /dev/null 2>&1
		j=$((j+1))
#		if [ $j = $node ]; then
#			break;
#		fi
	done

	j=0
  	for memory in `cat $memory_nodes`
  	do
  		ip=`echo $memory | cut -d ' ' -f1`
  		ssh -i ~/.ssh/id_rsa $ip killall benchmark > /dev/null 2>&1
  		j=$((j+1))
#  		if [ $j = $node ]; then
#  			break;
#  		fi
  	done

    IFS="$old_IFS"
}


run_thread_test() {
# thread test
echo "*********************run thread test**********************"
result_file=$bin/results/thread
node_range="8"
thread_range="1 2 3 4 5 6 7 8"
remote_range="0 50 100"
shared_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
read_range="0 50 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for shared_ratio in $shared_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_remote_test() {
# remote test
echo "**************************run remote test****************************"
result_file=$bin/results/remote_ratio
node_range="8"
thread_range="1"
remote_range="0 10 20 30 40 50 60 70 80 90 100"
shared_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
read_range="0 100" #"0 50 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for op_type in $op_range
do
for read_ratio in $read_range
do
for shared_ratio in $shared_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for remote_ratio in $remote_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_shared_test() {
# shared test
echo "**************************run shared test****************************"
result_file=$bin/results/shared_ratio
node_range="8"
thread_range="1"
remote_range="88"
shared_range="0 10 20 30 40 50 60 70 80 90 100"
read_range="50" #"0 50 70 80 90 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}

run_shared_test_noeviction() {
# shared test
echo "**************************run shared test****************************"
result_file=$bin/results/shared_ratio-noeviction
node_range="8"
thread_range="1"
remote_range="88"
shared_range="0 10 20 30 40 50 60 70 80 90 100"
read_range="50 100" #"0 50 70 80 90 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.5

for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_read_test() {
# read ratio test
echo "**************************run read ratio test****************************"
result_file=$bin/results/read_ratio
node_range="8"
thread_range="1"
remote_range="0 50 100"
shared_range="0"
read_range="0 10 20 30 40 50 60 70 80 90 100"
space_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for shared_ratio in $shared_range
do
for read_ratio in $read_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}

run_space_test() {
# space locality test
echo "**************************run space locality test****************************"
result_file=$bin/results/space_locality
node_range="8"
thread_range="1"
remote_range="100"
shared_range="0"
read_range="0 50 100"
space_range="0 10 20 30 40 50 60 70 80 90 100"
time_range="0" #"0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
done
done
done
}


run_time_test() {
# time locality test
echo "**************************run time locality test****************************"
result_file=$bin/results/time_locality
node_range="8"
thread_range="1"
remote_range="100"
shared_range="0"
read_range="0 50 100"
space_range="0"
time_range="0 10 20 30 40 50 60 70 80 90 100"
op_range="0 1 2 3"
cache_th=0.15

for remote_ratio in $remote_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
for node in $node_range
do
for thread in $thread_range
do
for shared_ratio in $shared_range
do
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
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
#node_range=$(wc -l < $compute_nodes)
thread_range="1 4 8 12 16"
remote_range="0" #"20 40 60 80 100"
shared_range="0 50 100"
read_range="50"
space_range="0"
time_range="0"
op_range="0"
#cache_th=0.5

for remote_ratio in $remote_range
do
for shared_ratio in $shared_range
do
for op_type in $op_range
do
for read_ratio in $read_range
do
for space_locality in $space_range
do
for time_locality in $time_range
do
#for node in $node_range
#do
  echo $node
for thread in $thread_range
do
#    remote_ratio=`echo "($node-1)*100/$node" | bc`
#    echo $remote_ratio
#    if [[ $node = 1 ]]; then
#        continue;
#    fi
	if [[ $remote_ratio -gt 0 && $node = 1 ]]; then
		continue;
	fi
    run
done
done
done
done
done
#done
done
done
}

#run_thread_test
#run_read_test
#run_time_test
#run_shared_test
#run_remote_test
#run_space_test
#run_shared_test_noeviction
run_node_test
