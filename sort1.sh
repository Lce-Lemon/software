#!/bin/bash

get_version() {
  local mode=$1
  local v1=$2
  local v2=$3

  if [ -z "$mode" ] || [ -z "$v1" ] || [ -z "$v2" ]; then
    echo "错误: 需要提供三个参数 (min/max, version1, version2)"
    return 1
  fi

  if [[ "$mode" != "min" && "$mode" != "max" ]]; then
    echo "错误: 第一个参数必须是 'min' 或 'max'"
    return 1
  fi

  # 按点分割版本号
  IFS='.' read -ra V1 <<<"$v1"
  IFS='.' read -ra V2 <<<"$v2"

  # 获取两个版本号的最大段数
  local max_parts=${#V1[@]}
  if [ ${#V2[@]} -gt $max_parts ]; then
    max_parts=${#V2[@]}
  fi

  # 逐段比较
  for ((i = 0; i < max_parts; i++)); do
    local part1=${V1[i]:-0}
    local part2=${V2[i]:-0}

    # 检查是否有前导零和前导零数量
    local has_leading_zero1=false
    local has_leading_zero2=false
    local leading_zeros1=0
    local leading_zeros2=0

    # 检查part1的前导零
    if [[ ${#part1} -gt 1 && $part1 =~ ^0 ]]; then
      has_leading_zero1=true
      # 计算前导零数量 - 使用更准确的方法
      temp1="$part1"
      leading_zeros1=0
      while [[ ${temp1:0:1} == "0" ]]; do
        leading_zeros1=$((leading_zeros1+1))
        temp1="${temp1:1}"
      done
    fi

    # 检查part2的前导零
    if [[ ${#part2} -gt 1 && $part2 =~ ^0 ]]; then
      has_leading_zero2=true
      # 计算前导零数量 - 使用更准确的方法
      temp2="$part2"
      leading_zeros2=0
      while [[ ${temp2:0:1} == "0" ]]; do
        leading_zeros2=$((leading_zeros2+1))
        temp2="${temp2:1}"
      done
    fi

    # 获取数值部分
    local num1=$((10#$part1))
    local num2=$((10#$part2))

    # 比较前导零规则（优先级最高）
    # 如果都有前导零，前导零数量少的版本大
    if [ "$has_leading_zero1" = true ] && [ "$has_leading_zero2" = true ]; then
      if [ $leading_zeros1 -lt $leading_zeros2 ]; then
        # v1的前导零少 => v1 > v2
        if [ "$mode" = "min" ]; then
          echo "$v2"
        else
          echo "$v1"
        fi
        return
      elif [ $leading_zeros1 -gt $leading_zeros2 ]; then
        # v1的前导零多 => v1 < v2
        if [ "$mode" = "min" ]; then
          echo "$v1"
        else
          echo "$v2"
        fi
        return
      fi
      # 前导零数量相等，继续比较数值
    fi
    # 有前导零 vs 无前导零的比较
    if [ "$has_leading_zero1" = true ] && [ "$has_leading_zero2" = false ]; then
      # v1有前导零，v2没有 => v1 < v2
      if [ "$mode" = "min" ]; then
        echo "$v1"
      else
        echo "$v2"
      fi
      return
    elif [ "$has_leading_zero1" = false ] && [ "$has_leading_zero2" = true ]; then
      # v1没有前导零，v2有 => v1 > v2
      if [ "$mode" = "min" ]; then
        echo "$v2"
      else
        echo "$v1"
      fi
      return
    fi

    # 最后比较数值大小（都没有前导零，或前导零数量相等时）
    if [ $num1 -lt $num2 ]; then
      # v1 < v2
      if [ "$mode" = "min" ]; then
        echo "$v1"
      else
        echo "$v2"
      fi
      return
    elif [ $num1 -gt $num2 ]; then
      # v1 > v2
      if [ "$mode" = "min" ]; then
        echo "$v2"
      else
        echo "$v1"
      fi
      return
    fi
  done

  # 所有段都相等，返回第一个版本号
  echo "$v1"
}

# 测试函数
test_get_version() {
  echo "=== 版本号比较测试 ==="

  # 测试用例：[版本1, 版本2, 期望的最小值, 期望的最大值]
  test_cases=(
    "1.2.3 1.2.4 1.2.3 1.2.4"
    "1.2.3 1.2.03 1.2.03 1.2.3"
    "1.02.3 1.2.3 1.02.3 1.2.3"
    "1.002.3 1.02.3 1.002.3 1.02.3"
    "1.2.3.4 1.2.3 1.2.3 1.2.3.4"
    "2.0.0 1.9.9 1.9.9 2.0.0"
    "01.2.3 1.2.3 01.2.3 1.2.3"
    "1.2.03 1.2.3 1.2.03 1.2.3"
    "0.0.1 0.0.01 0.0.01 0.0.1"
    "1.02.3 1.002.3 1.002.3 1.02.3"
    "1.0.00106 1.0.01 1.0.00106 1.0.01"
  )
  
  for case in "${test_cases[@]}"; do
    read -r v1 v2 expected_min expected_max <<< "$case"
    
    actual_min=$(get_version "min" "$v1" "$v2")
    actual_max=$(get_version "max" "$v1" "$v2")
    
    # 检查结果是否符合预期
    min_status="✓"
    max_status="✓"
    
    if [ "$actual_min" != "$expected_min" ]; then
      min_status="✗ (期望: $expected_min)"
    fi
    
    if [ "$actual_max" != "$expected_max" ]; then
      max_status="✗ (期望: $expected_max)"
    fi
    
    echo "测试: $v1 vs $v2"
    echo "  最小值: $actual_min $min_status"
    echo "  最大值: $actual_max $max_status"
    echo ""
  done
}

# 主程序入口
if [ $# -eq 0 ]; then
  # 如果没有参数，运行测试
  test_get_version
else
  # 如果有参数，执行get_version函数
  get_version "$@"
fi