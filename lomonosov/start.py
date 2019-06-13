#!/usr/bin/python
# Before start do it
# module add openmpi/4.0.0-icc slurm/15.08.1 java/jdk1.8.0
# HowTo sbatch -p test -n 10 --ntasks-per-node 1 --output=test.out run ./start.py 280 280 5 10GB

import os
import subprocess
import time
import sys


def FileCheck(fn):
    try:
        f = open(fn, "r")
        f.close()
        return 1
    except IOError:
        return 0


if (len(sys.argv) != 6):
    print('--partitions=?')
    print('--total-executor-cores=?"')
    print('--executor-cores=?')
    print('--executor-memory=?')
    print('--graphName=?')
    sys.exit(-1)

rank = os.environ["SLURM_PROCID"]
jobid = os.environ["SLURM_JOB_ID"]
n = int(os.environ["SLURM_JOB_NUM_NODES"])

if rank == "0":
    # Starting master
    pathToAddress = subprocess.check_output(["./spark/sbin/start-master.sh"]).split(" ")[4][:-1]
    time.sleep(10)

    # Writing master's address to file "jobid + /master_started"
    subprocess.check_output(["mkdir", jobid])
    file = open(pathToAddress, "r")
    s = file.read()
    file.close()
    try:
        masterAddress = s.split("\n")[14].split(" ")[8]
    except:
        print(s)
        sys.exit(-1)

    file = open(jobid + "/master_started", "w")
    file.write(masterAddress)
    file.close()

    print("Master started! " + masterAddress + " " + pathToAddress)

    # Wait until all slaves started
    cnt = 1
    while cnt != n:
        cnt += FileCheck(jobid + "/slave_started_" + str(cnt))
        time.sleep(2)

    # Your spark submits
    # output = subprocess.check_output(["./spark/bin/spark-submit",
    #                                   "--conf", "spark.default.parallelism=" + sys.argv[1],
    #                                   "--conf", "spark.sql.shuffle.partitions=" + sys.argv[1],
    #                                   "--class", "SparkPi",
    #                                   "--master", masterAddress,
    #                                   "--total-executor-cores", sys.argv[2],
    #                                   "--executor-cores", sys.argv[3],
    #                                   "--executor-memory", sys.argv[4],
    #                                   "sparkpi.jar", sys.argv[1]])
    try:
        output = subprocess.check_output(["./spark/bin/spark-submit",
                                        "--conf", "spark.default.parallelism=" + sys.argv[1],
                                        "--conf", "spark.sql.shuffle.partitions=" + sys.argv[1],
                                        "--conf", "spark.driver.extraClassPath=graphframes-0.7.0-spark2.4-s_2.11.jar",
                                        "--conf", "spark.executor.extraClassPath=graphframes-0.7.0-spark2.4-s_2.11.jar",
                                        "--class", "com.wrapper.BoruvkaAlgorithm",
                                        "--master", masterAddress,
                                        "--total-executor-cores", sys.argv[2],
                                        "--executor-cores", sys.argv[3],
                                        "--executor-memory", sys.argv[4],
                                        "boruvka_2.11-0.1-SNAPSHOT.jar", sys.argv[5]])
        print(output)
    except:
        subprocess.check_output(["touch", jobid + "/finish"])
        subprocess.check_output(["./spark/sbin/stop-master.sh"])
        time.sleep(10)
        subprocess.check_output(["touch", jobid + "/master_stoped"])
        print(sys.exc_info()[0])
        sys.exit(-1)

    # Create finish file to signal slaves to stop
    subprocess.check_output(["touch", jobid + "/finish"])

    # Wait until all slaves stoped
    cnt = 1
    while cnt != n:
        cnt += FileCheck(jobid + "/slave_stoped_" + str(cnt))
        time.sleep(2)

    # Stoping master
    subprocess.check_output(["./spark/sbin/stop-master.sh"])
    time.sleep(10)
    subprocess.check_output(["touch", jobid + "/master_stoped"])
else:
    # Wait until master started and read master's address
    while(not FileCheck(jobid + "/master_started")):
        time.sleep(2)
    file = open(jobid + "/master_started", "r")
    masterAddress = file.read()
    file.close()

    # Starting slave
    subprocess.check_output(["./spark/sbin/start-slave.sh", masterAddress])
    time.sleep(10)

    # Create file to signal master that slave has started
    print("Slave started!")
    subprocess.check_output(["touch", jobid + "/slave_started_" + rank])

    # Wait for finish file
    while(not FileCheck(jobid + "/finish")):
        time.sleep(10)

    # Stoping slave and signal master
    subprocess.check_output(["./spark/sbin/stop-slave.sh"])
    time.sleep(10)
    subprocess.check_output(["touch", jobid + "/slave_stoped_" + rank])