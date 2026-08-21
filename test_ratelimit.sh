#!/bin/bash

BASE_URL="http://localhost:8080/api/v1/ratelimit"

echo "========== 分布式限流与配额管理模块测试 =========="
echo ""

echo "【1/10】查看所有策略"
curl -s "$BASE_URL/policies" | python3 -m json.tool 2>/dev/null || curl -s "$BASE_URL/policies"
echo ""

echo "【2/10】查看所有调用方"
curl -s "$BASE_URL/callers" | python3 -m json.tool 2>/dev/null || curl -s "$BASE_URL/callers"
echo ""

echo "【3/10】service-alpha 请求 20 个令牌 (令牌桶算法)"
curl -s -X POST "$BASE_URL/callers/service-alpha/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 20}' | python3 -m json.tool 2>/dev/null
echo ""

echo "【4/10】service-gamma 请求 35 个令牌 (超过30配额，应该被限流)"
curl -s -X POST "$BASE_URL/callers/service-gamma/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 35}' | python3 -m json.tool 2>/dev/null
echo ""

echo "【5/10】service-beta 借 15 个配额给 service-gamma"
curl -s -X POST "$BASE_URL/borrow" \
  -H "Content-Type: application/json" \
  -d '{"from_caller": "service-beta", "to_caller": "service-gamma", "amount": 15}' | python3 -m json.tool 2>/dev/null
echo ""

echo "【6/10】借用后 service-gamma 状态"
curl -s "$BASE_URL/callers/service-gamma" | python3 -m json.tool 2>/dev/null
echo ""

echo "【7/10】service-gamma 再次请求 35 个令牌 (借用后应该可以)"
curl -s -X POST "$BASE_URL/callers/service-gamma/request" \
  -H "Content-Type: application/json" \
  -d '{"tokens": 35}' | python3 -m json.tool 2>/dev/null
echo ""

echo "【8/10】service-alpha 配额热调整到 200"
curl -s -X POST "$BASE_URL/callers/service-alpha/adjust" \
  -H "Content-Type: application/json" \
  -d '{"new_quota_limit": 200}' | python3 -m json.tool 2>/dev/null
echo ""

echo "【9/10】全局统计"
curl -s "$BASE_URL/stats" | python3 -m json.tool 2>/dev/null
echo ""

echo "【10/10】service-alpha 的限流历史"
curl -s "$BASE_URL/callers/service-alpha/history?limit=5" | python3 -m json.tool 2>/dev/null
echo ""

echo "========== 测试完成 =========="
echo ""
echo "提示："
echo "  - 观察令牌桶填充: watch -n 1 'curl -s $BASE_URL/callers/service-alpha | python3 -m json.tool'"
echo "  - 查看活跃借用: curl -s $BASE_URL/borrows | python3 -m json.tool"
echo "  - 归还配额: curl -X POST $BASE_URL/return -H 'Content-Type: application/json' -d '{\"from_caller\": \"service-gamma\", \"to_caller\": \"service-beta\", \"amount\": 15}'"
