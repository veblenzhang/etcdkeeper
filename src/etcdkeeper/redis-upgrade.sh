#!/bin/sh

export ETCDCTL_API=2
etcd_endpoints="http://"
current_path=$(dirname `readlink -f $BASH_SOURCE`)
declare -A node_map
declare -A slave_node_map
declare -a finish_machines

function log() {
    local ts=`date "+%F %T"`
    echo  "$ts $*" 
}

function log_c() {
    local ts=`date "+%F %T"`
    echo -n "$ts "
    echo -e "\033[1;32m$*\033[0m" 
}

function error() {
    echo -e "\033[1;31m$1\033[0m"
    if [ ${#machine_list[@]} -ne 0 ]; then
        log "机器列表:"
        log ${machine_list[*]}
    fi

    if [ ${#finish_machines[@]} -ne 0 ]; then
        log "完成列表:"
        log ${finish_machines[*]}
    fi


}


function write_memory() {
	echo -n "$1:$2 " >> ${path}/memory.list
	used=`./redis-cli -h $1 -p $2 $pwd info memory | grep used_memory: | awk -F ":" '{print $2}' | tr -d "\r"` 
	echo -n "$used " >> ${path}/memory.list
	maxmemory=`./redis-cli -h $1 -p $2 $pwd config get maxmemory | grep -v "maxmemory"`
	echo -n "$maxmemory" >> ${path}/memory.list
    echo "" >> ${path}/memory.list
}

function list_machine_node() {
    log_c "------------------------------------"
    log "当前集群的主从分布情况："
    ./redis-cli -c -h $1 -p $2  $pwd cluster nodes | awk  -F: '{print $1 " " $2}' | awk  ' {if ($4 ~ /master/){freq[$2, "master"]++; machine[$2]++} else if($4 ~ /slave/){freq [$2,"slave"]++;machine[$2]++} else{freq [$2, $4]++;machine[$2]++} } END{printf("%-15s %8s %8s \n" ,"machine", "master", "slave"); for (var in machine) { if (freq[var, "master"] !=  freq[var, "slave"]) {printf("\033[1;31m%-15s %8.2f %8.2f \033[0m\n", var, freq[var, "master"], freq[var, "slave"])} else { printf("\033[1;32m%-15s %8.2f %8.2f \033[0m\n", var, freq[var, "master"], freq[var, "slave"])}}}' 
    ./redis-cli -c -h $1 -p $2  $pwd cluster nodes | awk  -F: '{print $1 " " $2}' | awk  ' {if ($4 ~ /master/){freq[$2, "master"]++; machine[$2]++} else if($4 ~ /slave/){freq [$2,"slave"]++;machine[$2]++} else{freq [$2, $4]++;machine[$2]++} } END{printf("%-15s %8s %8s \n" ,"machine", "master", "slave"); for (var in machine) { if (freq[var, "master"] !=  freq[var, "slave"]) {printf("%-15s %8.2f %8.2f \n", var, freq[var, "master"], freq[var, "slave"])} else { printf("%-15s %8.2f %8.2f \n", var, freq[var, "master"], freq[var, "slave"])}}}' > ${path}/machine.list
}

function check_node() {
    list_machine_node $1 $2
    log_c "------------------------------------"
    log "开始进行集群检查..."
    log "连通性..."
    PONG=`./redis-cli -c -h $1 -p $2  $pwd ping`
    if [ "${PONG}"x != PONGx ]; then
        error "无法连接 "$1":"$2
        exit 1
    fi 
    log "连通性 OK..."   

    log "主从节点分布..."
    # 红色表示master和slave在同一台主机上
    ./redis-cli -c -h $1 -p $2  $pwd  cluster nodes | awk '{if ($4 != "-" ){$1 = $4} hash[$1]++; if($3 ~ /master/){ split($2, masternode, "@") ; freq[$1, "master"]=masternode[1]; hashSlot[$1] = $9} else if($3 ~ /slave/){split($2, slavenode, "@") ; freq[$1, "slave"]=slavenode[1] } else{freq [$1, $3]=$2}} END{printf("%-40s %-20s %-20s %-20s\n" ,"hashCode", "master", "slave", "hashSlot"); for (var in hash) {split(freq[var, "master"], master, ":"); master_ip = master[1] ;  split(freq[var, "slave"], slave, ":") ;slave_ip = slave[1] ;  if (master_ip == slave_ip) { printf("\033[1;31m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var]) } else if (slave_ip == '') { printf("\033[1;31m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var]) } else {  printf("\033[1;32m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var])}}}'
    ./redis-cli -c -h 10.31.52.18 -p 2883 -a AccsService@123  cluster nodes | awk '{if ($4 != "-" ){$1 = $4} hash[$1]++; if($3 ~ /master/){ split($2, masternode, "@") ; freq[$1, "master"]=masternode[1]; hashSlot[$1] = $9} else if($3 ~ /slave/){split($2, slavenode, "@") ; freq[$1, "slave"]=slavenode[1] } else{freq [$1, $3]=$2}} END{printf("%-40s %-20s %-20s %-20s\n" ,"hashCode", "master", "slave", "hashSlot"); for (var in hash) {split(freq[var, "master"], master, ":"); master_ip = master[1] ;  split(freq[var, "slave"], slave, ":") ;slave_ip = slave[1] ;  if (master_ip == slave_ip) { printf("\033[1;31m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var]) } else if (slave_ip == '') { printf("\033[1;31m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var]) } else {  printf("\033[1;32m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var])}}}'
    check_count=`cat ${path}/cluster_node.info | grep "1;31m" | wc -l`
    if [ ${check_count} -ne 0 ]; then
        error "检查失败，存在一主从在同一主机上："
        cat ${path}/cluster_node.info
        exit 1
    fi
    log "主从节点分布 OK..."

    log "节点配置信息..."
    ./redis-cli -h $1 -p $2 $pwd cluster nodes | awk '{print $2}' | awk -F '@' '{print $1}' | sed 's/:/ /g' | while read ip port;do ./redis-cli -h $ip -p $port $pwd cluster nodes | sed 's/myself,//g' | sort | awk '{$4=null;$5=null;$6=null;$7=null;print $0}' > ${path}/${ip}_${port}.config;done
    config_count=`ls ${path}/*.config | grep -v total | awk '{print $NF}' | xargs sha256sum | awk '{print $1}' | sort | uniq -c | wc -l`
    if [ ${config_count} -ne 1 ]; then
        error "检查失败，存在集群节点配置不相同。检查业务是否正在扩缩容，或者存在失败节点"
        exit 1
    fi
    log "节点配置信息 OK..."

    # 保存集群的内存
    log "集群内存检查..."
    ./redis-cli -h $1 -p $2 $pwd cluster nodes | awk '{print $2}' | awk -F '@' '{print $1}' | sed 's/:/ /g'|while read ip port;do  write_memory $ip $port ;done

    # 校验
    while read line
    do
        arr=($(echo $line | awk 'BEGIN{FS=",";OFS=" "} {print $1,$2,$3}'))
        size=${#arr[@]}

        # 不等于3，认为内存获取失败
        if [ 3 -ne ${size} ]; then
            echo "${arr[0]} 内存校验失败"
            exit 1
        fi
        used=${arr[1]}
        maxmeory=${arr[2]}
        rate=`expr $used '*' 100 '/' $maxmeory`
        if [ $rate -gt 60 ]
        then 
            error "${arr[0]} 内存校验失败 , 当前内存： ${rate}%"
            exit 1
        fi
    done < ${path}/memory.list
    log "集群内存检查 OK..."
    log "集群检查 OK..."
}

function save_node() {
    log_c "------------------------------------"
    log "获取主机列表...."
    str=(`cat ${path}/cluster_node.info | awk 'NR>1 {printf ("%s,%s\n"), $2,$3}'`)

    # 保存原始信息
    for i in ${!str[@]} 
    do
        # 赋值
        eval $(echo ${str[i]} | awk '{split($0, filearray, ",");print "node_map["filearray[1]"]="filearray[2]}')
        eval $(echo ${str[i]} | awk '{split($0, filearray, ",");print "slave_node_map["filearray[2]"]="filearray[1]}')
    done 

    machine_list=($(cat ${path}/machine.list | awk 'NR>1 {print $1}'))
    log "主机列表："
    log_c ${machine_list[*]}

    # 检测主机连通性
    for machine in ${machine_list[@]}
    do
        log "Test dcs@${machine}..."
        timeout 3 ssh dcs@${machine} echo "ssh access!"
        if [[ $? -ne 0 ]]; then
            error "SSH [dcs@${machine}] has no access!"
            exit 1
        fi
    done   

    # 检测主机sudo权限
    for machine in ${machine_list[@]}
    do
        auright=`ssh -o "StrictHostKeyChecking no"  dcs@${machine} "sudo -A echo abc"`
        if [ "$auright" != "abc" ]; then
            error "${machine} don't have sudo right,check fail!!"
            exit -1
        else
            log "${machine} sudo test ok"
        fi
    done   
}

function get_nodes() {
    entry_info=`./etcdctl --endpoints=${etcd_endpoints} ls ${cluster_adddress}/ENTRY | head -1`
    if [ "$entry_info"x == "x" ]; then
        error "集群不存在...."
        exit 1
    fi
    read o_ip o_port  <<< `log ${entry_info} | awk -F '/' '{print $6}' |  awk -F ':' '{print $1 ,$2}'`
    check_node ${o_ip} ${o_port}
    save_node
}

function check_switch() {
    log "$1:$2 连通性..."
    PONG=`./redis-cli -c -h $1 -p $2  $pwd ping`
    if [ "${PONG}"x != PONGx ]; then
        error "无法连接 "$1":"$2
        exit 1
    fi 
    log "$1:$2 连通性 OK..."   

    size=${#node_map[@]}
    slave_size=${#slave_node_map[@]}
    while true
    do
        log "$1:$2 等待主节点间同步..."
        current=`./redis-cli -h $1 -p $2 $pwd cluster nodes | grep -v disconnected | grep master | wc -l`
        if [ ${current} -eq ${size} ]; then
            log "$1:$2 主节点间同步完成..."
            break
        fi
        sleep 1
    done

    while true
    do
        log "$1:$2 等待从节点间同步..."
        current=`./redis-cli -h $1 -p $2 $pwd cluster nodes | grep -v disconnected | grep slave | wc -l`
        if [ ${current} -eq ${slave_size} ]; then
            log "$1:$2 从节点间同步完成..."
            break
        fi
        sleep 1
    done

    while true
    do
        log "$1:$2 集群间节点配置信息校验..."
        rm -f ${path}/*.config
        ./redis-cli -h $1 -p $2 $pwd cluster nodes | awk '{print $2}' | awk -F '@' '{print $1}' | sed 's/:/ /g' | while read ip port;do ./redis-cli -h $ip -p $port $pwd cluster nodes | sed 's/myself,//g' | sort | awk '{$4=null;$5=null;$6=null;$7=null;print $0}' > ${path}/${ip}_${port}.config;done
        config_count_tmp=`ls ${path}/*.config | grep -v total | awk '{print $NF}' | xargs sha256sum | awk '{print $1}' | sort | uniq -c | wc -l`
        if [ ${config_count_tmp} -eq 1 ]; then
            log "$1:$2 集群间节点配置信息 OK..."
            break
        fi
        sleep 1
    done
}

function check() {
    if [ ${dc_name}x == ''x ]; then 
        log 'dc_name cant be empty'
        exit 1
    fi
    if [ ${cluster_name}x == ''x ]; then 
        log 'cluster_name cant be empty'
        exit 1
    fi
}

function init_path() {
    # 清空文件夹
    path=${current_path}/${dc_name}/${cluster_name}
    if [ ! -d "${path}" ]; then
        mkdir -p ${path}
    else 
        rm -rf ${path}/*
    fi

}

function fail_over() {
    log "$3:$4 -> slave | $1:$2 -> master  | 准备"
    # 进行failover
    result=`./redis-cli -c -h $1 -p $2  $pwd cluster failover`
    if [ "${result}" != "OK" ] ;then
        error "$3:$4 -> slave | $1:$2 -> master  | 失败 | ${result}"
        exit 1
    fi

    let count=0
    # 检查failover是否完成
    while true
    do
        ./redis-cli -c -h $3 -p $4  $pwd cluster nodes  | grep myself | grep slave
        if [ $? -eq 0 ] ;then
            # 已经切换为从节点
            log "$3:$4 -> slave | 完成"
            ./redis-cli -c -h $1 -p $2  $pwd cluster nodes | grep myself | grep master
            if [ $? -eq 0 ] ;then
                # 已经切换为从节点
                log "$1:$2 -> master | 完成"
                break
            fi
        fi

        log "$3:$4 正在切换...."
        let count+=1
        # 强制进行切换
        if [ "$(($count % 10))" = "4" ]; then
            log "$3:$4 强制切换...."
            fail_over_force $1 $2 $3 $4
        fi
        sleep 2
    done    
    log_c "$1:$2 切换成功"
}

function fail_over_force() {
    # 进行failover
    result=`./redis-cli -c -h $1 -p $2  $pwd cluster failover FORCE`
    if [ "${result}" != "OK" ] ;then
        error "$3:$4 -> slave | $1:$2 -> master  | 失败"
        exit 1
    fi 
}

function restart_machine() {
    log "开始重启 : $1"
    ssh -o "StrictHostKeyChecking no" dcs@$1 "sudo reboot 2 >/dev/null 2>&1 &"
    log "$1 正在重启中... "
    sleep 20
    while true
    do
        PONG=`./redis-cli -c -h $1 -p $2  $pwd ping 2>/dev/null`
        if [ "${PONG}"x == PONGx ]; then
            break
        fi 
        log "$1 正在重启中... "
        sleep 5
    done
    log "$1 重启完成... "
}

function check_slave_status() { 
    while true
    do 
        # loadin不为0说明当前节点不可用
        ./redis-cli -c -h $1 -p $2  $pwd info persistence | grep "loading:0"

        if [[ $? == 0 ]] ; then
            break
        fi
        log "$1:$2节点正在初始化......"
        sleep 2
    done
}

function restart() {
    log "------------------------------------"
    for machine in ${machine_list[@]}
    do
        log_c "${machine} 主机开始切换"
        # 清空
        unset node_map_new
        unset slave_node_map_new
        declare -A node_map_new
        declare -A slave_node_map_new

        ./redis-cli -c -h $o_ip -p $o_port  $pwd  cluster nodes | awk '{if ($4 != "-" ){$1 = $4} hash[$1]++; if($3 ~ /master/){ split($2, masternode, "@") ; freq[$1, "master"]=masternode[1]; hashSlot[$1] = $9} else if($3 ~ /slave/){split($2, slavenode, "@") ; freq[$1, "slave"]=slavenode[1] } else{freq [$1, $3]=$2}} END{printf("%-40s %-20s %-20s %-20s\n" ,"hashCode", "master", "slave", "hashSlot"); for (var in hash) {split(freq[var, "master"], master, ":"); master_ip = master[1] ;  split(freq[var, "slave"], slave, ":") ;slave_ip = slave[1] ;  if (master_ip == slave_ip) { printf("\033[1;31m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var]) } else {  printf("\033[1;32m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var])}}}'> ${path}/cluster_node_2.info
        # 针对新的集群信息进行解析
        str_new=(`cat ${path}/cluster_node_2.info | awk 'NR>1 {printf ("%s,%s\n"), $2,$3}'`)
        log ${str_new[*]}
        for i in ${!str_new[@]} 
        do
            # 赋值
            eval $(echo ${str_new[i]} | awk '{split($0, filearray_new, ",");print "node_map_new["filearray_new[1]"]="filearray_new[2]}')
            # eval $(echo ${str_new[i]} | awk '{split($0, filearray_new, ",");print "slave_node_map_new["filearray_new[2]"]="filearray_new[1]}')
        done 
        
        switch=false
        for key in ${!node_map_new[@]}
        do  
            master_node=(${key//:/ }) 
            if [[ ${master_node[0]} == ${machine} ]]
            then
                switch=true
                node_port=${master_node[1]}
                slave_node=(${node_map_new[$key]//:/ }) 
                check_slave_status ${slave_node[0]} ${slave_node[1]}
                fail_over ${slave_node[0]} ${slave_node[1]} ${master_node[0]} ${master_node[1]}
                #log ${key}"-${node_map[$key]}"
            fi
        done
        log "${machine} 主机待重启，集群信息校验..."
        if [ "$switch" = true ]; then
            check_switch ${machine} ${node_port}
        fi
        log "${machine} 主机待重启，集群信息校验Ok..."
        restart_machine ${machine} ${node_port}
        #sleep 10
        log_c "${machine} 主机重启已完成，集群信息校验..."
        if [ "$switch" = true ]; then
            check_switch ${machine} ${node_port}
        else 
            check_switch $o_ip $o_port
        fi
        log_c "${machine} 主机已完成，集群信息信息校验Ok..."
        log "已完成机器列表:"
        finish_machines=("${finish_machines[@]}" ${machine})
        log ${finish_machines[*]}
    done
    sleep 10
    log_c "集群[${dc_name}.${cluster_name}]所有机器重启 OK"
}

function restore() {
    log "------------------------------------"
    log "开始对集群进行修复"
    for machine in ${machine_list[@]}
    do
        ./redis-cli -c -h $o_ip -p $o_port  $pwd  cluster nodes | awk '{if ($4 != "-" ){$1 = $4} hash[$1]++; if($3 ~ /master/){ split($2, masternode, "@") ; freq[$1, "master"]=masternode[1]; hashSlot[$1] = $9} else if($3 ~ /slave/){split($2, slavenode, "@") ; freq[$1, "slave"]=slavenode[1] } else{freq [$1, $3]=$2}} END{printf("%-40s %-20s %-20s %-20s\n" ,"hashCode", "master", "slave", "hashSlot"); for (var in hash) {split(freq[var, "master"], master, ":"); master_ip = master[1] ;  split(freq[var, "slave"], slave, ":") ;slave_ip = slave[1] ;  if (master_ip == slave_ip) { printf("\033[1;31m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var]) } else {  printf("\033[1;32m%-40s %-20s %-20s %-20s \033[0m\n", var, freq[var, "master"], freq[var, "slave"], hashSlot[var])}}}'> ${path}/cluster_node_2.info

        unset node_map_new
        unset slave_node_map_new
        declare -A node_map_new
        declare -A slave_node_map_new

        # 针对新的信息进行解析
        str_new=(`cat ${path}/cluster_node_2.info | awk 'NR>1 {printf ("%s,%s\n"), $2,$3}'`)
        for i in ${!str_new[@]} 
        do
            # 赋值
            eval $(echo ${str_new[i]} | awk '{split($0, filearray_new, ",");print "node_map_new["filearray_new[1]"]="filearray_new[2]}')
            # eval $(echo ${str_new[i]} | awk '{split($0, filearray_new, ",");print "slave_node_map_new["filearray_new[2]"]="filearray_new[1]}')
        done 

        log "开始对 "${machine}" 进行修复"
        switch=false
        for key in ${!node_map_new[@]}
        do  
            master_node_new=(${key//:/ }) 
            if [[ ${master_node_new[0]} == ${machine} ]]
            then
                switch=true
                node_new_port=${master_node_new[1]}
                slave_node_new=(${node_map_new[$key]//:/ }) 

                # 如果新的主节点在原始信息找不到就进行切换
                if [ ! -n "${node_map[$key]}" ]; then
                    log "准备修复: $key"
                    fail_over ${slave_node_new[0]} ${slave_node_new[1]} ${master_node_new[0]} ${master_node_new[1]}
                    log "修复完成: $key"
                fi
            fi
        done
        log_c "${machine} 主机修复已完成，进行集群信息同步和信息校验..."
        if [ "$switch" = true ]; then
            check_switch ${machine} ${node_new_port[1]}    
        else
            check_switch $o_ip $o_port    
        fi
        log_c "${machine} 主机修复已完成，集群信息同步和信息校验Ok..."
    done
    log "机器列表:"
    log_c ${machine_list[*]}
    log "完成列表:"
    log_c ${finish_machines[*]}
    log_c "集群[${dc_name}.${cluster_name}]升级完成"
}

dc_name=$1
cluster_name=$2
# 读取密码
read pwd

function main(){
    # 检查集群名
    check
    cluster_adddress=/${dc_name}/SERVICE/${cluster_name}
    init_path

    if [ "${pwd}"x != x ]; then 
        pwd='-a '${pwd}
    fi

    get_nodes
    restart
    restore
}

main
