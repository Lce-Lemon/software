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
      # 计算前导零数量
      temp1="$part1"
      leading_zeros1=0
      while [[ ${temp1:0:1} == "0" ]]; do
        leading_zeros1=$((leading_zeros1 + 1))
        temp1="${temp1:1}"
      done
    fi

    # 检查part2的前导零
    if [[ ${#part2} -gt 1 && $part2 =~ ^0 ]]; then
      has_leading_zero2=true
      # 计算前导零数量
      temp2="$part2"
      leading_zeros2=0
      while [[ ${temp2:0:1} == "0" ]]; do
        leading_zeros2=$((leading_zeros2 + 1))
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

# 详细的版本比较函数（返回数值结果）
compare_versions_detailed() {
  local v1="$1"
  local v2="$2"

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
      temp1="$part1"
      leading_zeros1=0
      while [[ ${temp1:0:1} == "0" ]]; do
        leading_zeros1=$((leading_zeros1 + 1))
        temp1="${temp1:1}"
      done
    fi

    # 检查part2的前导零
    if [[ ${#part2} -gt 1 && $part2 =~ ^0 ]]; then
      has_leading_zero2=true
      temp2="$part2"
      leading_zeros2=0
      while [[ ${temp2:0:1} == "0" ]]; do
        leading_zeros2=$((leading_zeros2 + 1))
        temp2="${temp2:1}"
      done
    fi

    # 获取数值部分
    local num1=$((10#$part1))
    local num2=$((10#$part2))

    # 比较前导零规则（优先级最高）
    if [ "$has_leading_zero1" = true ] && [ "$has_leading_zero2" = true ]; then
      if [ $leading_zeros1 -lt $leading_zeros2 ]; then
        echo 1
        return
      elif [ $leading_zeros1 -gt $leading_zeros2 ]; then
        echo -1
        return
      fi
    fi

    # 有前导零 vs 无前导零的比较
    if [ "$has_leading_zero1" = true ] && [ "$has_leading_zero2" = false ]; then
      echo -1
      return
    elif [ "$has_leading_zero1" = false ] && [ "$has_leading_zero2" = true ]; then
      echo 1
      return
    fi

    # 最后比较数值大小
    if [ $num1 -lt $num2 ]; then
      echo -1
      return
    elif [ $num1 -gt $num2 ]; then
      echo 1
      return
    fi
  done

  # 所有段都相等
  echo 0
}

# 版本号排序函数
sort_versions() {
  local order="$1"
  shift
  local versions=("$@")

  if [ -z "$order" ]; then
    echo "错误: 需要指定排序方式 (asc/desc)"
    return 1
  fi

  if [[ "$order" != "asc" && "$order" != "desc" ]]; then
    echo "错误: 排序方式必须是 'asc' 或 'desc'"
    return 1
  fi

  if [ ${#versions[@]} -eq 0 ]; then
    echo "错误: 没有提供版本号"
    return 1
  fi

  # 使用冒泡排序对版本号进行排序
  local n=${#versions[@]}
  local sorted_versions=("${versions[@]}")

  for ((i = 0; i < n; i++)); do
    for ((j = 0; j < n - i - 1; j++)); do
      local v1="${sorted_versions[j]}"
      local v2="${sorted_versions[j + 1]}"

      # 比较两个版本号
      local comparison_result=$(compare_versions_detailed "$v1" "$v2")

      # 根据排序方式决定是否交换
      local should_swap=false
      if [ "$order" = "asc" ] && [ $comparison_result -gt 0 ]; then
        should_swap=true
      elif [ "$order" = "desc" ] && [ $comparison_result -lt 0 ]; then
        should_swap=true
      fi

      if [ "$should_swap" = true ]; then
        # 交换元素
        local temp="${sorted_versions[j]}"
        sorted_versions[j]="${sorted_versions[j + 1]}"
        sorted_versions[j + 1]="$temp"
      fi
    done
  done

  # 输出排序结果
  for version in "${sorted_versions[@]}"; do
    echo "$version"
  done
}

# 从文件名中提取版本号的函数
extract_version_from_filename() {
  local filename="$1"

  # 支持多种版本号格式的正则表达式
  local version=""

  # 尝试不同的版本号模式
  if [[ $filename =~ [vV]?([0-9]+(\.[0-9]+){2,3}) ]]; then
    version="${BASH_REMATCH[1]}"
  elif [[ $filename =~ version[_-]?([0-9]+(\.[0-9]+){2,3}) ]]; then
    version="${BASH_REMATCH[1]}"
  elif [[ $filename =~ update[_-]?[vV]?([0-9]+(\.[0-9]+){2,3}) ]]; then
    version="${BASH_REMATCH[1]}"
  elif [[ $filename =~ ([0-9]+(\.[0-9]+){2,3}) ]]; then
    version="${BASH_REMATCH[1]}"
  fi

  echo "$version"
}

# 获取文件夹中所有带版本号的SQL文件并排序
get_sql_files_by_version() {
  local directory="$1"
  local order="${2:-asc}"
  local pattern="${3:-*.sql}"

  if [ -z "$directory" ]; then
    echo "错误: 需要提供目录路径"
    return 1
  fi

  if [ ! -d "$directory" ]; then
    echo "错误: 目录 '$directory' 不存在"
    return 1
  fi

  if [[ "$order" != "asc" && "$order" != "desc" ]]; then
    echo "错误: 排序方式必须是 'asc' 或 'desc'"
    return 1
  fi

  # 存储带版本号的文件
  local version_files=()
  local versions=()

  # 使用关联数组存储版本号到文件路径的映射
  declare -A version_to_file

  # 遍历目录中的文件（包括子目录）
  while IFS= read -r -d '' file; do
    local basename=$(basename "$file")
    local version=$(extract_version_from_filename "$basename")

    if [ -n "$version" ]; then
      # 如果版本号已存在，跳过重复
      if [[ -z "${version_to_file[$version]}" ]]; then
        versions+=("$version")
        version_to_file["$version"]="$file"
      fi
    fi
  done < <(find "$directory" -name "$pattern" -type f -print0)

  if [ ${#versions[@]} -eq 0 ]; then
    echo "在目录 '$directory' 中没有找到带版本号的文件（模式: $pattern）" >&2
    return 1
  fi

  # 对版本号进行排序
  local sorted_versions
  readarray -t sorted_versions < <(
    # 将版本号数组传递给排序函数
    printf '%s\n' "${versions[@]}" | {
      local temp_array=()
      while IFS= read -r line; do
        temp_array+=("$line")
      done

      # 调用现有的排序函数
      sort_versions "$order" "${temp_array[@]}"
    }
  )

  # 输出排序后的文件路径
  for version in "${sorted_versions[@]}"; do
    echo "${version_to_file[$version]}"
  done
}

# 显示SQL文件的详细信息
show_sql_files_info() {
  local directory="$1"
  local order="${2:-asc}"
  local pattern="${3:-*.sql}"

  echo "=== SQL文件版本信息 ==="
  echo "目录: $directory"
  echo "模式: $pattern"
  echo "排序: $order"
  echo ""

  local files
  readarray -t files < <(get_sql_files_by_version "$directory" "$order" "$pattern")

  if [ ${#files[@]} -eq 0 ]; then
    return 1
  fi

  local max_version_len=0
  local max_filename_len=0

  # 计算最大长度用于格式化
  for file in "${files[@]}"; do
    local basename=$(basename "$file")
    local version=$(extract_version_from_filename "$basename")

    if [ ${#version} -gt $max_version_len ]; then
      max_version_len=${#version}
    fi

    if [ ${#basename} -gt $max_filename_len ]; then
      max_filename_len=${#basename}
    fi
  done

  # 显示格式化的文件信息
  printf "%-${max_version_len}s | %-${max_filename_len}s | %s\n" "版本号" "文件名" "完整路径"
  printf "%*s-+-%*s-+-%s\n" $max_version_len "" $max_filename_len "" "----------"

  for file in "${files[@]}"; do
    local basename=$(basename "$file")
    local version=$(extract_version_from_filename "$basename")
    local size=""

    if [ -f "$file" ]; then
      size=$(du -h "$file" 2>/dev/null | cut -f1)
    fi

    printf "%-${max_version_len}s | %-${max_filename_len}s | %s %s\n" "$version" "$basename" "$file" "($size)"
  done
}

# 从标准输入读取版本号列表并排序
sort_versions_from_stdin() {
  local order="$1"
  local versions=()

  if [ -z "$order" ]; then
    echo "错误: 需要指定排序方式 (asc/desc)"
    return 1
  fi

  # 读取标准输入
  while IFS= read -r line; do
    if [ -n "$line" ]; then
      versions+=("$line")
    fi
  done

  if [ ${#versions[@]} -eq 0 ]; then
    echo "错误: 没有从标准输入读取到版本号"
    return 1
  fi

  sort_versions "$order" "${versions[@]}"
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
    read -r v1 v2 expected_min expected_max <<<"$case"

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

# 使用示例和帮助信息
show_usage() {
  echo "版本号比较和排序工具"
  echo ""
  echo "用法:"
  echo "  $0 min|max version1 version2                    # 比较两个版本号"
  echo "  $0 sort asc|desc version1 version2 ...         # 排序多个版本号"
  echo "  $0 sort-stdin asc|desc                          # 从标准输入排序"
  echo "  $0 sql-files directory [asc|desc] [pattern]     # 获取SQL文件并按版本排序"
  echo "  $0 sql-info directory [asc|desc] [pattern]      # 显示SQL文件详细信息"
  echo "  $0 extract-version filename                     # 从文件名提取版本号"
  echo "  $0 test                                         # 运行测试"
  echo "  $0 help                                         # 显示帮助"
  echo ""
  echo "示例:"
  echo "  $0 max 1.2.3 1.2.03                            # 输出: 1.2.3"
  echo "  $0 sort asc 1.02.3 1.2.3 1.002.4               # 按升序排列"
  echo "  $0 sql-files ./migrations desc                  # 获取migration目录中SQL文件，降序排列"
  echo "  $0 sql-files ./sql asc '*.sql'                  # 指定文件模式"
  echo "  $0 sql-info ./updates                           # 显示SQL文件详细信息"
  echo "  $0 extract-version 'update_v1.2.3.sql'         # 输出: 1.2.3"
  echo ""
  echo "SQL文件版本号识别模式:"
  echo "  - v1.2.3, V1.2.3"
  echo "  - version_1.2.3, version-1.2.3"
  echo "  - update_v1.2.3, update-1.2.3"
  echo "  - 1.2.3 (直接包含版本号)"
  echo "  - migration_1.02.4.sql"
  echo "  - V1.0.01_create_table.sql"
  echo ""
  echo "版本号比较规则:"
  echo "  1. 前导零数量少的版本更大"
  echo "  2. 有前导零的版本小于无前导零的版本"
  echo "  3. 数值大小比较（最低优先级）"
  echo ""
  echo "注意事项:"
  echo "  - sql-files 会递归搜索指定目录及其子目录"
  echo "  - 不包含版本号的文件会被忽略"
  echo "  - 相同版本号的文件只会显示第一个找到的"
}

# 主程序入口
case "${1:-}" in
"min" | "max")
  if [ $# -ne 3 ]; then
    echo "错误: min/max 需要提供两个版本号参数"
    exit 1
  fi
  get_version "$@"
  ;;
"sort")
  if [ $# -lt 3 ]; then
    echo "错误: sort 需要提供排序方式和至少一个版本号"
    exit 1
  fi
  sort_versions "${@:2}"
  ;;
"sort-stdin")
  if [ $# -ne 2 ]; then
    echo "错误: sort-stdin 需要提供排序方式参数"
    exit 1
  fi
  sort_versions_from_stdin "$2"
  ;;
"sql-files")
  if [ $# -lt 2 ]; then
    echo "错误: sql-files 需要提供目录参数"
    exit 1
  fi
  get_sql_files_by_version "$2" "${3:-asc}" "${4:-*.sql}"
  ;;
"sql-info")
  if [ $# -lt 2 ]; then
    echo "错误: sql-info 需要提供目录参数"
    exit 1
  fi
  show_sql_files_info "$2" "${3:-asc}" "${4:-*.sql}"
  ;;
"extract-version")
  if [ $# -ne 2 ]; then
    echo "错误: extract-version 需要提供文件名参数"
    exit 1
  fi
  extract_version_from_filename "$2"
  ;;
"test")
  test_get_version
  ;;
"help" | "-h" | "--help")
  show_usage
  ;;
"")
  # 默认运行测试
  test_get_version
  ;;
*)
  echo "错误: 未知命令 '$1'"
  echo "使用 '$0 help' 查看帮助信息"
  exit 1
  ;;
esac
