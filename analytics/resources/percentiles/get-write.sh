for s in {cassandra,cockroach,sconekv}; do 
    for w in {a,b,c,f}; do 
        for t in {16,32,64,128,256}; do 
            timestamp=`grep -m 1 "50 sec" ../$s/workload$w/$t/*.out | awk '{print $2}' | head -c 8`
            for file in ../$s/workload$w/$t/*node*.csv; do
                grep -A 200 $timestamp $file | awk -F "\"*,\"*" '{print $13/1000}' >> write/write-$s-workload$w-$t.dat
            done
        done
    done
done
