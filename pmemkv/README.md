<!--
Copyright (c) 2015 - 2021 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# PmemKV Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a PmemKV. It uses the PmemKV Java bindings.

## Quickstart

### 1. Install PmemKV Java binding
Optionally compile and install the [Java binding](https://github.com/pmem/pmemkv-java) with its dependencies:
```shell
export JAVA_HOME=#PATH_TO_YOUR_JAVA_HOME
git clone https://github.com/pmem/pmemkv-java.git
cd pmemkv-java
mvn install
```

### 2. Set up YCSB
You need to clone the repository and compile PmemKV module.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -pl site.ycsb:pmemkv-binding -am package
```

### 3. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb.sh load pmemkv -P workloads/workloada -p pmemkv.engine=cmap -p pmemkv.dbsize=DB_SIZE -p pmemkv.dbpath=/path/to/pmem/pool
```

Then, you can run the workload:

```
bin/ycsb.sh run pmemkv -P workloads/workloada -p pmemkv.engine=cmap -p pmemkv.dbsize=DB_SIZE -p pmemkv.dbpath=/path/to/pmem/pool
```

## Configuration Options
Driver has a few configuration options to parametrize path, size and engine using the following:

| Parameter     | Meaning        | Obligatory |
| :-----------: | -------------- | :--------: |
| pmemkv.engine | Storage engine, possible values are listed [here](https://github.com/pmem/pmemkv#storage-engines) | N |
| pmemkv.dbsize | Pool file size | Y          |
| pmemkv.dbpath | Pool file path | Y          |
