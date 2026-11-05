#!/usr/bin/env bash
# Hides Ivy resolution chatter and JVM startup noise from console output.
exec grep -v -E --line-buffered 'WARN |^\s*$|[Ii]vy|^::|^\s+confs:|^\s+found |artifacts|modules|^\s*---|^\s+\|| conf  |default log|Using Spark|To adjust logging|Setting default|in \[default\]|central in|^\s+:: '
