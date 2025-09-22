

## Requirements
- Go v1.17 >=
- gcc

### Windows
#### (1) [msys2](https://www.msys2.org/) install 
####  (2) Open `MSYS2 MinGW 64-bit` terminal and run the following commands
refer to this [issue](https://github.com/confluentinc/confluent-kafka-go/issues/889)
```bash
$ pacman -Syu
$ pacman -S base-devel
$ pacman -S mingw-w64-x86_64-toolchain
$ pacman -Q -e
$ pacman -S mingw-w64-ucrt-x86_64-gcc
```
#### Add PATH
Add `C:\msys64\mingw64\bin` to your system PATH environment variable.



## Usage

### Stop the application
⚠️You should **stop** the application producing data to the source topic before running this tool.

### Execute CLI
```bash
$ go main.go --env {env} --source_topic <source_topic_name> --target_topic <target_topic_name>
```
