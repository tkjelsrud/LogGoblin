# GoLogMonitor
Log/File monitoring programmed in Go - post changes (patterns, like errors) to a service (jsonbin)

Can run continiously (deamon) or one-shot.
Will scan directories or spesfied log files for new/changed content and look for patterns such as ERROR, then report findings back to a reporting dashboard (through InfluxDB or Json BIN).

Use case: simplified log monitoring across VMs/distributed environments where installing larger enterprise monitoring systems is not an option.