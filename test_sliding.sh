#!/bin/bash

BASE="http://localhost:8080/api/v1/ratelimit"

echo "========================================"
echo "  滑动窗口连续衰减测试"
echo "  窗口: 10秒, max: 100"
echo "========================================"
echo ""

echo "创建策略和调用方..."
curl -s -X POST "$BASE/policies" \
  -H "Content-Type: application/json" \
  -d '{"name": "slide-test", "algorithm": "sliding_window", "window_sec": 10, "max_tokens": 100}' > /dev/null

curl -s -X POST "$BASE/callers/bind" \
  -H "Content-Type: application/json" \
  -d '{"caller_id": "slide-caller", "policy_name": "slide-test", "quota_limit": 100}' > /dev/null

echo "一次性请求 80 个令牌 (消耗 80)"
curl -s -X POST "$BASE/callers/slide-caller/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 80}' > /dev/null

echo ""
echo "时间点     已用配额    剩余配额"
echo "--------------------------------"

get_status() {
  curl -s "$BASE/callers/slide-caller" | python3 -c "
import sys, json
d = json.load(sys.stdin)['caller']
print(f'{d[\"used_tokens\"]:>4}       {d[\"remaining\"]:>4}')
"
}

echo -n "t=0s     "
get_status

sleep 2
echo -n "t=2s     "
get_status

sleep 2
echo -n "t=4s     "
get_status

sleep 2
echo -n "t=6s     "
get_status

sleep 2
echo -n "t=8s     "
get_status

sleep 2
echo -n "t=10s    "
get_status

sleep 2
echo -n "t=12s    "
get_status

echo ""
echo "========================================"
echo "预期: 已用配额应该随时间平滑下降"
echo "而不是到10秒时突然跳到0"
echo "========================================"
