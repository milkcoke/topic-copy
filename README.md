

## Requirements
- Go v1.17 >=
- MSVC


### Windows
```bash
$ choco install visualstudio2022buildtools --package-parameters "--add Microsoft.VisualStudio.Workload.VCTools --includeRecommended --includeOptional"
```

### Mac & Linux


## Usage
```bash
$ go main.go --env {env} --source_topic <source_topic_name> --target_topic <target_topic_name>
```
