#! /bin/bash

for i in 01 02 03 04 05 06 07 08 09 10
do
    ssh-copy-id -i ~/.ssh/id_rsa.pub $USER@fa20-cs425-g07-$i.cs.illinois.edu
done