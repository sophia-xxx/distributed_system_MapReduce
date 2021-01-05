# 分布式系统 MapReduce
Implementation of a distributed file system. We use UDP to detect failures and TCP to execute the file commands.

## Usage

* To run on local host:

```bash
** go run main.go debug **
```

* To run across machines:
```bash
** go run main.go **
```

## Commands
* To deploy a intro node

```bash
** join intro**
```

* To deploy a ordinary node

```bash
** join [port_number]**
```

* To leave the system

```bash
** leave **
```
* To get node id

```bash
** id **
```
* To list members of the system

```bash
** members **
```
* To put a file

```bash
** put [local_filepath] [filename] **
```

* To get a file

```bash
** get [filename] [local_filepath] **
```

* To delete a file

```bash
** delete [filename] **
```

* To get a list of servers with a  file

```bash
** ls [filename] **
```

* To get a list of files on the server

```bash
** store  **
```
* To run Map command

```bash
** maple <maple_exe> <maple_num> <sdfs_prefix> <sdfs_src_file>**
```
Before run maple command, better to put sdfs_src file into sdfs system. 

For example, first run  `put mj_exe/vote_input test`, then run `maple mj_exe/maple_vote 3 sdfs_pre test`


* To run Reduce command

```bash
** juice <juice_exe> <juice_num> <sdfs_prefix> <dest_file> <delete_flag(0/1)> <shuffle method (hash/range)>**
```
The `juice_num` and `sdfs_prefix`  should be the same with maple command.
