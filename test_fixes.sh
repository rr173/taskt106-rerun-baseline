#!/bin/bash

BASE="http://localhost:8080/api/v1/ratelimit"

echo "========================================"
echo "  限流模块三个问题修复验证"
echo "========================================"
echo ""

echo "【问题1: 策略 max_tokens 是否生效?】"
echo ""
echo "创建策略: max_tokens=20 的令牌桶"
curl -s -X POST "$BASE/policies" \
  -H "Content-Type: application/json" \
  -d '{"name": "verify-max", "algorithm": "token_bucket", "max_tokens": 20, "refill_rate": 2, "refill_unit": "per_second"}' > /dev/null

echo "绑定调用方: quota_limit=100 (但策略 max 是 20)"
curl -s -X POST "$BASE/callers/bind" \
  -H "Content-Type: application/json" \
  -d '{"caller_id": "verify-max-caller", "policy_name": "verify-max", "quota_limit": 100}' > /dev/null

echo -n "  调用方剩余配额: "
curl -s "$BASE/callers/verify-max-caller" | python3 -c "
import sys, json
d = json.load(sys.stdin)['caller']
print(f\"remaining={d['remaining']}, quota_limit={d['quota_limit']}, policy_max={d['policy_max']}\")
"
echo "  预期: remaining=20 (因为策略 max_tokens=20，覆盖了调用方的 100)"
echo ""

echo "【问题2: 排队等待功能?】"
echo ""
echo "先把 20 个令牌用完"
curl -s -X POST "$BASE/callers/verify-max-caller/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 20}' > /dev/null

echo "请求 10 个 (用完了，直接拒绝):"
result=$(curl -s -X POST "$BASE/callers/verify-max-caller/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 10}')
echo -n "  "
echo "$result" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(f\"allowed={r['allowed']}, granted={r['granted']}, reason={r.get('reason','')}\")
"

echo "请求 10 个，waitable=true (进入队列):"
result=$(curl -s -X POST "$BASE/callers/verify-max-caller/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 10, "waitable": true, "wait_sec": 60}')
echo -n "  "
echo "$result" | python3 -c "
import sys, json
r = json.load(sys.stdin)
print(f\"allowed={r['allowed']}, queued={r.get('queued',False)}, position={r.get('position',0)}\")
"

echo -n "  当前等待队列长度: "
curl -s "$BASE/wait-queue" | python3 -c "
import sys, json
q = json.load(sys.stdin).get('wait_queue', [])
print(len(q))
"

echo ""
echo "等待 6 秒 (令牌桶以 2/s 填充，6秒后应该够 10 个了)..."
sleep 6

echo -n "  6秒后等待队列长度: "
curl -s "$BASE/wait-queue" | python3 -c "
import sys, json
q = json.load(sys.stdin).get('wait_queue', [])
print(len(q))
"
echo "  (预期: 0，因为队列中的请求被自动放行了)"
echo ""

echo "【问题3: 滑动窗口是否按时间连续衰减?】"
echo ""
echo "创建滑动窗口策略: 窗口10秒，max=100"
curl -s -X POST "$BASE/policies" \
  -H "Content-Type: application/json" \
  -d '{"name": "verify-sliding", "algorithm": "sliding_window", "window_sec": 10, "max_tokens": 100}' > /dev/null

echo "绑定调用方"
curl -s -X POST "$BASE/callers/bind" \
  -H "Content-Type: application/json" \
  -d '{"caller_id": "verify-sliding-caller", "policy_name": "verify-sliding", "quota_limit": 100}' > /dev/null

echo "立即请求 60 个令牌"
curl -s -X POST "$BASE/callers/verify-sliding-caller/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 60}' > /dev/null

echo -n "  已用配额: "
curl -s "$BASE/callers/verify-sliding-caller" | python3 -c "
import sys, json
d = json.load(sys.stdin)['caller']
print(f\"used={d['used_tokens']}, remaining={d['remaining']}\")
"

echo ""
echo "等待 5 秒 (半个窗口)..."
sleep 5

echo -n "  5秒后已用配额: "
curl -s "$BASE/callers/verify-sliding-caller" | python3 -c "
import sys, json
d = json.load(sys.stdin)['caller']
print(f\"used={d['used_tokens']}, remaining={d['remaining']}\")
"
echo "  (预期: 已用配额应该约为 30，因为滑动窗口按时间衰减了一半)"
echo ""
echo "再等 5 秒 (一整个窗口)..."
sleep 5

echo -n "  10秒后已用配额: "
curl -s "$BASE/callers/verify-sliding-caller" | python3 -c "
import sys, json
d = json.load(sys.stdin)['caller']
print(f\"used={d['used_tokens']}, remaining={d['remaining']}\")
"
echo "  (预期: 已用配额接近 0，因为整个窗口都过去了)"
echo ""
echo "========================================"
echo "  测试完成"
echo "========================================"
