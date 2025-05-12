# schedule_bench
作业调度系统时延基准评测工具

概述

本工具用于测试不同系统负载水平下的作业调度性能。它能够自动化提交测试作业、监控执行情况，并分析不同负载条件下的调度延迟。

功能特点

• 自动化作业提交：支持可配置的作业参数

• 动态负载控制：带安全阈值的智能负载调节

• 全面监控：实时跟踪作业状态和调度延迟

• 专业分析报告：包含详细性能数据和可视化图表

• PDF报告生成：自动生成包含统计摘要和图表的专业报告

• 补充作业管理：智能维持目标负载水平


系统要求

• Python 3.6+

• 必需Python包：

  • `sqlite3` (内置)

  • `matplotlib`

  • `pandas`

  • `seaborn`

  • `fpdf2`

  • `python-dateutil`

• SLURM作业调度系统(用于作业提交和监控)


安装步骤

1. 克隆仓库或下载源代码

使用方法

1. 准备配置文件(JSON格式)，示例如下：

```json
{
    "queue_name": "your_queue_name",
    "load_levels": [0.3, 0.5, 0.7, 0.9],
    "num_jobs": 50,
    "node_options": [1, 2, 4],
    "max_cores_per_node": 16
}
```

2. 运行基准测试：

```bash
python schedule_bench.py config.json
```

配置参数说明

| 参数名 | 类型 | 描述 | 默认值 |
|--------|------|------|--------|
| queue_name | string | 测试队列名称 | 无(必填) |
| load_levels | array | 要测试的负载水平列表(0-1之间) | 无(必填) |
| num_jobs | integer | 每个负载水平下要提交的作业数 | 无(必填) |
| node_options | array | 作业使用的节点数选项 | [1,2,4] |
| max_cores_per_node | integer | 每个节点的最大核心数 | 自动检测 |
| db_name | string | 数据库文件名 | "scheduling.db" |
| monitor_interval | float | 监控间隔(秒) | 5 |
| max_retries | integer | 最大重试次数 | 3 |
| sleep_time | integer | 测试作业的睡眠时间(秒) | 1 |

输出结果

测试完成后将生成：
1. SQLite数据库文件(包含所有作业记录)
2. 控制台性能摘要报告
3. PDF格式的详细测试报告("scheduling_benchmark_report.pdf")
4. 可视化图表("combined_visualization.png")

注意事项

1. 运行前请确保有足够的队列资源
2. 测试过程中会自动提交补充作业来维持负载
3. 测试结束后会自动清理所有补充作业
4. 建议在测试队列空闲时运行基准测试

技术支持

如有任何问题，请联系开发者或提交issue。
