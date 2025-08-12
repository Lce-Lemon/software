#!/bin/bash

# 初始化未安装包的数组
uninstalled=()

# 遍历当前目录下的所有 RPM 文件
for f in *.rpm; do
    # 提取 RPM 包名（不含版本信息）
    pkg_name=$(rpm -qp --queryformat '%{NAME}' "$f" 2>/dev/null)

    # 检查是否成功提取包名
    if [ -z "$pkg_name" ]; then
        echo "警告: 无法解析 $f 的包名，跳过"
        continue
    fi

    # 检查包是否已安装（任何版本）
    if ! rpm -q "$pkg_name" >/dev/null 2>&1; then
        echo "添加到安装列表: $f ($pkg_name)"
        uninstalled+=("$f")
    else
        installed_version=$(rpm -q "$pkg_name")
        echo "已安装跳过: $f (已安装版本: $installed_version)"
    fi
done

# 如果有未安装的包，则用 dnf 安装
if [ ${#uninstalled[@]} -gt 0 ]; then
    echo "正在安装 ${#uninstalled[@]} 个未安装的包..."
    sudo dnf install --setopt=localpkg_gpgcheck=0 "${uninstalled[@]}"
else
    echo "所有 RPM 包均已安装，无需操作。"
fi

echo "退出状态码：${$?}"