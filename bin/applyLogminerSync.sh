#!/bin/bash

curl -X POST http://localhost:8102/applyLogminerSync -H 'Content-Type: application/json' -d '{"resetOffset":"false","startScn":"","applyOrDrop":1,"tableListStr":"TGLMINER.TM_HEARTBEAT"}'
