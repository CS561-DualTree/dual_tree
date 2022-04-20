#!/bin/bash
WORKLOAD_GENERATOR="workload_generator.o"
if [ ! -f "$WORKLOAD_GENERATOR" ]
then
	echo "Use \"make\" command before run the script, because the file \"workload_generator.o\" is needed"
	exit 1
fi

if [ $# -ne 1 ] 
then
	echo "Only accept one of the following arguments(in upper case): [100K, 1M, 10M, 50M]"
	exit 1
fi

case $1 in

	100K)
		data_size="100000"
		;;
			
	1M)
		data_size="1000000"
		;;

	10M)
		data_size="10000000"
		;;

	50M)
		data_size="50000000"
		;;

	*)
		echo "Invalid data size ${1}"
		exit 1
		;;
esac

test_root_dir="./test_set/"
if [ ! -d "$test_root_dir" ]
then
	mkdir "$test_root_dir"
fi

test_set_dir="${1}_test/"
if [ ! -d "$test_root_dir$test_set_dir" ]
then
	mkdir "$test_root_dir$test_set_dir"
fi

dst_prefix="$test_root_dir$test_set_dir"
k_test_dir="${dst_prefix}k_test/"
l_test_dir="${dst_prefix}l_test/"

if [ ! -d "$k_test_dir" ]
then
	mkdir "$k_test_dir"
fi

if [ ! -d "$l_test_dir" ]
then
	mkdir "$l_test_dir"
fi
echo "Produce test datasets of size of ${data_size} keys"

ktest_prefix="${k_test_dir}k"
ltest_prefix="${l_test_dir}l"
lvalue=50
kvalue=35
for j in {1..5}
	do
		for i in {10..50..10}
			do
				./workload_generator.o $data_size $i $lvalue
				./workload_generator.o $data_size $kvalue $i
		done
	# sleep enough time to wait for workload_generator.o finishing and changing the random seed
		sleep 1m
done


for i in {10..50..10}
	do
		if [ ! -d "$ktest_prefix$i" ]
		then
			mkdir "$ktest_prefix$i"
		fi
		if [ ! -d "$ltest_prefix$i" ]
		then
			mkdir "$ltest_prefix$i"
		fi
		mv "data_${data_size}-elems_${i}-kperct_${lvalue}-lperct"* "$ktest_prefix$i"
		mv "data_${data_size}-elems_${kvalue}-kperct_${i}-lperct"* "$ltest_prefix$i"
done
