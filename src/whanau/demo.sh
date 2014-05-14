#!/bin/bash

go test --run TestDemo 2>/dev/null | egrep -v 'EOF|connection|broken'