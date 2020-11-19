# mp2
Implementation of a distributed file system. We use UDP to detect failures and TCP to execute the file commands.

## Installation

Build and deploy with

```bash
**Make all**
```

## Usage

To run on local host:

```bash
** go run main.go debug **
```

To run across machines:
```bash
** go run main.go **
```

## Commands
To deploy a intro node
```bash
** join intro**
```

To deploy a ordinary node
```bash
** join [port_number]**
```

To leave the system
```bash
** leave **
```
To get node info
```bash
** id **
```
To list members of the system
```bash
** members **
```
To put a file
```bash
** put [local_filepath] [filename] **
```

To get a file
```bash
** get [filename] [local_filepath] **
```

To delete a file
```bash
** delete [filename] **
```

To get a list of servers with a  file
```bash
** ls [filename] **
```

To get a list of files on the server
```bash
** store  **
```