# Fast DDS 2.6.11 研究分支说明

本目录和 `research/*` 分支用于比赛项目研究，不属于上游 Fast DDS 2.6.11 的正式功能。

## 分支关系

```text
v2.6.11: 87dd60c
  |
  +-- research/scan-retransmission-trace: 50f9db8
        |
        +-- research/adaptive-retransmission-v1: 4de1055 (2026-07-18 检查点)
```

| 分支 | 职责 | 状态 |
|---|---|---|
| `research/scan-retransmission-trace` | 可选 CSV 重传事件插桩和有限 ROS 2 benchmark | 已验证的观测基线；只接受必要修复 |
| `research/adaptive-retransmission-v1` | 显式启用的 Reliable StatefulWriter 远端 Reader 旧样本恢复控制 | 当前开发分支；shadow 检查点为 `4de1055`，后续提交实现无 GAP 动态准入 |

`adaptive-retransmission-v1` 包含 `scan-retransmission-trace` 的全部提交。日常开发和 Linux 编译应使用前者，
不需要在两个分支之间来回切换。标准源码对照使用 `git show 87dd60c:path/to/file`，也不需要切换分支。

## 远端约定

推荐统一使用：

```text
origin    https://github.com/eProsima/Fast-DDS.git
personal  https://github.com/ahahahahahahhahahah/Fast-DDS.git
```

个人 fork 保留了大量上游分支。不要直接执行无参数的 `git fetch personal`。将默认 fetch 范围限制到研究分支：

```bash
git config --unset-all remote.personal.fetch || true
git config --add remote.personal.fetch \
  '+refs/heads/research/*:refs/remotes/personal/research/*'
git config remote.personal.tagOpt --no-tags
git fetch --prune personal
```

以后更新当前开发分支只需：

```bash
git switch research/adaptive-retransmission-v1
git pull --ff-only
```

## 当前 V1 边界

```text
显式开启的用户数据 Reliable synchronous StatefulWriter
+ matched_remote_readers_
+ ACKNACK/NACK_FRAG 旧样本恢复
```

V1 不修改 Best Effort、进程内通信、Data Sharing、Writer 类型选择和 RTPS 协议，不使用 GAP。
Transport 不是功能开关；只有可控丢包实验才会选择 UDP/netem 作为测试工具。

异步 Writer 在主动 V1 中回退原始 `SEND_NOW`，因为只限制旧样本准入不能修复 Flow Controller 的新队列
绝对优先问题。支持异步 Writer 时必须同时实现旧队列最低服务份额或 aging。

## 测量工具

- `ros2_adaptive_benchmark/`：正式 C++ `ament_cmake` 包，用于同机默认 Transport 和双机性能测量；
- `ros2_udp_benchmark/`：早期 Python 冒烟工具和可选的单机 UDP 故障注入配置。

主要性能结果使用 C++ 工具并关闭重传 CSV trace。Python/UDP-only 工具只用于快速诊断，不作为最终数据源。
