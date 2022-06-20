#!/bin/bash
#注意=左右无空格
datasizes=(10000 100000)
dims=(128)
ks=(10 100)
ms=(8)
alphas=(10)
main(){
    dropdb test
    createdb test 
    rm log
    touch log
    for datasize in ${datasizes[@]}
    do    
        for dim in ${dims[@]}
        do 
            for k in ${ks[@]}
            do
                for m in ${ms[@]}
                do 
                    for alpha in ${alphas[@]}
                    do
                        echo -e "\n\n[param] datasize: ${datasize}, dim: ${dim}, k: ${k}, m: ${m}, alpha: ${alpha}\n\n" | tee -a log
                        sed -e "s/@datasize/${datasize}/g"\
                        -e "s/@dim/${dim}/g"\
                        -e "s/@len/$(($dim/8))/g"\
                        -e "s/@k/${k}/g"\
                        -e "s/@m/${m}/g"\
                        -e "s/@alpha/${alpha}/g" test.sql | psql test 2>&1 | tee -a log
                    done
                done 
            done
        done
    done
}

main