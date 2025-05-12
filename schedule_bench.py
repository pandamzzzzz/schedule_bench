import argparse
import json
import subprocess
import re
import time
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from datetime import datetime
from dateutil.parser import parse
import os 
from fpdf import FPDF
import matplotlib as mpl
from fpdf.enums import XPos, YPos
import math
from collections import Counter
import numpy as np
import random
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

class SchedulerBenchmark:
    def __init__(self, config_path):
        self.config = self.load_config(config_path)
        self.init_db()
        self.current_stage_index = 0  # 跟踪当前测试阶段
        
    def load_config(self, config_path):
        """Load configuration file"""
        with open(config_path) as f:
            config = json.load(f)
        
        # Set defaults
        defaults = {
            "db_name": "scheduling.db",
            "monitor_interval": 5,
            "max_retries": 3,
            "final_check_cycles": 10,
            "sleep_time": 1,
            "initial_interval": 1.0,
            "adjust_rate": 0.9,
            "min_interval": 0.5,
            "max_interval": 5.0,
            "monitor_interval": 3,
            "final_check_cycles": 20,
            "buffer_threshold": 0.005,
            "min_load_adjust_interval": 0.5,
            "max_adjust_wait": 120,
            "supplemental_sleep": 3600
        }
        return {**defaults, **config}
    
    def init_db(self):
        """Initialize database"""
        conn = sqlite3.connect(self.config["db_name"])
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_metrics (
                load_ratio REAL,
                job_id TEXT PRIMARY KEY,
                submit_time REAL,
                start_time REAL,
                delay REAL,
                retries INTEGER DEFAULT 0,
                is_supplemental INTEGER DEFAULT 0,
                nodes INTEGER,
                tasks_per_node INTEGER,
                exclusive TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def get_queue_allocation(self):
        """获取队列节点分配情况，排除drain/draining/down等不可用节点"""
        try:
            cmd = f"sinfo -p {self.config['queue_name']} -h -o '%D|%T'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            
            total_available_nodes = 0  # 可用节点总数(排除drain/down等)
            allocated_nodes = 0        # 已分配节点数
            
            # 定义不可用节点状态列表
            unavailable_states = ['down', 'drain', 'draining', 'fail', 'failed', 'error']
            # 定义已分配节点状态列表
            allocated_states = ['allocated', 'mixed', 'allocated+', 'completing']
            
            for line in result.stdout.strip().split('\n'):
                if not line.strip():
                    continue
                
                # 解析节点数量和状态（示例输入："1|mix"）
                node_count, state = line.split('|')
                node_count = int(node_count)
                state = state.strip().lower()
                
                # 排除不可用状态的节点
                if not any(unavail_state in state for unavail_state in unavailable_states):
                    total_available_nodes += node_count
                    
                    # 检查是否是已分配状态
                    if any(alloc_state in state for alloc_state in allocated_states):
                        allocated_nodes += node_count
                        
                # 调试信息（可选）
                # print(f"[状态分组] 节点数: {node_count} 状态: {state}")

            print(f"[Resource Stats] Available nodes: {total_available_nodes} Allocated nodes: {allocated_nodes}")
            return allocated_nodes, total_available_nodes
            
        except Exception as e:
            print(f"Resource check failed: {str(e)}")
            return 0, 0

    def submit_test_job(self, sleep_time=None, is_supplemental=0, load_ratio=None):
        """改进的作业提交方法，支持补充作业标记"""
        sleep_time = sleep_time or self.config["sleep_time"]
        try:
            # 获取节点核心数信息（用于补充作业和单节点随机化）
            
            def get_node_cores(queue_name):
                try:
                    # 使用 scontrol 命令获取 partition 信息
                    cmd = f"scontrol show partition {queue_name}"
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

                    # 打印结果以查看输出内容
                    #print("scontrol command output:\n", result.stdout)

                    # 使用正则表达式精确匹配 MaxCPUsPerNode=数字 格式
                    match = re.search(r'MaxCPUsPerNode=(\d+)', result.stdout)
                    
                    if match:
                        return int(match.group(1))  # 返回匹配的核心数
                    else:
                        return 1  # 默认值，找不到时返回1
                except Exception as e:
                    print(f"Error: {e}")
                    return 1  # 默认值，如果发生错误
            if 'max_cores_per_node' in self.config:
                max_cores_per_node = self.config['max_cores_per_node']
            else:
                max_cores_per_node = get_node_cores(self.config['queue_name'])
            print(f"max_cores_per_node : {max_cores_per_node}")
            
            if is_supplemental:
                # 补充作业使用固定参数：单节点、满核心、独占
                nodes = 1
                ntasks_per_node = max_cores_per_node
                exclusive_flag = "#SBATCH --exclusive"  # 独占参数的正确写法
            else:
                # 普通测试作业使用随机化参数
                node_options = self.config['node_options']
                nodes = random.choice(node_options)
                
                # 随机化每节点任务数
                if nodes == 1:
                    # 单节点时随机选择1到最大核心数
                    ntasks_per_node = random.randint(1, max_cores_per_node)
                    exclusive_flag = ""  # 单节点不独占
                else:
                    # 多节点时固定为1任务/节点
                    ntasks_per_node = max_cores_per_node
                    exclusive_flag = "#SBATCH --exclusive"  # 多节点独占
            
            # 创建作业脚本
            script_lines = [
                "#!/bin/bash",
                f"#SBATCH --partition={self.config['queue_name']}",
                f"#SBATCH --nodes={nodes}",
                f"#SBATCH --ntasks-per-node={ntasks_per_node}",
                exclusive_flag,  # 独占参数（可能为空字符串）
                f"sleep {sleep_time}"
            ]
            # 过滤掉空行（当exclusive_flag为空时）
            script = "\n".join(line for line in script_lines if line.strip())
        
            # 提交作业
            with open("tmp_supplemental_job.sh", "w") as f:
                f.write(script)
                
            result = subprocess.run(
                "sbatch --parsable tmp_supplemental_job.sh",
                shell=True, 
                capture_output=True, 
                text=True, 
                check=True
            )
            job_id = result.stdout.strip()
            job_type = "supplemental" if is_supplemental else "test"
            print(f"[Job Submission] Successfully submitted {job_type} job ID: {job_id}")
            print(f"[SLURM Parameters] Nodes: {nodes}, Tasks/Node: {ntasks_per_node}, "
                f"Exclusive: {'yes' if exclusive_flag else 'no'}")

            # 插入数据库记录
            submit_time = time.time()
            conn = sqlite3.connect(self.config["db_name"])
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO job_metrics (load_ratio, job_id, submit_time, is_supplemental, 
                                    nodes, tasks_per_node, exclusive)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (load_ratio, job_id, submit_time, is_supplemental, 
                nodes, ntasks_per_node, bool(exclusive_flag)))
            conn.commit()
            conn.close()
            
            return job_id
                
        except subprocess.CalledProcessError as e:
            print(f"Job submission failed: {e.stderr}")
            return None
        except Exception as e:
            print(f"Database error: {str(e)}")
            return None

    def monitor_jobs(self):
        """Monitor job status"""
        conn = sqlite3.connect(self.config["db_name"])
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT job_id FROM job_metrics WHERE start_time IS NULL")
            pending_jobs = [row[0] for row in cursor.fetchall()]
            
            for job_id in pending_jobs:
                try:
                    cmd = f"sacct -j {job_id} --format=JobID,Start,State --noheader"
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
                    
                    for line in result.stdout.split('\n'):
                        if line.strip() and not line.startswith(' '):
                            parts = line.split()
                            if parts[0] == job_id and len(parts) > 2:
                                state = parts[2]
                                if state in ['CANCELLED', 'FAILED']:
                                    cursor.execute('''
                                        UPDATE job_metrics 
                                        SET start_time = ?, delay = -1 
                                        WHERE job_id = ?
                                    ''', (time.time(), job_id))

                                if state not in ['PENDING', 'CONFIGURING'] and parts[1] != 'Unknown':
                                    try:
                                        start_time = parse(parts[1]).timestamp()
                                        cursor.execute('''
                                            UPDATE job_metrics 
                                            SET start_time = ?, delay = ? - submit_time 
                                            WHERE job_id = ?
                                        ''', (start_time, start_time, job_id))
                                        conn.commit()
                                    except Exception as e:
                                        print(f"Time parsing error: {e}")
                except Exception as e:
                    print(f"Job monitoring failed: {e}")
                    cursor.execute('''
                        UPDATE job_metrics 
                        SET retries = retries + 1 
                        WHERE job_id = ?
                    ''', (job_id,))
                    conn.commit()
        finally:
            conn.close()

    def ensure_load_level(self, target_load):
        """改进的负载预检方法"""
        print(f"\n[Precheck] Target load: {target_load*100}%")
        
        # 初始资源检查
        allocated, total_nodes = self.get_queue_allocation()
        if total_nodes == 0:
            print("Error: No available nodes in queue")
            return
        
        current_load = allocated / total_nodes
        print(f"Current load: {current_load:.1%} ({allocated}/{total_nodes} nodes)")
        
        # 负载已达标时跳过
        if current_load >= target_load:
            print("Minimum load requirement already met")
            return
        
        # 计算需要补充的作业量（考虑节点独占性）
        needed_jobs = math.ceil(target_load * total_nodes - allocated)
        print(f"Need to submit {needed_jobs} supplemental jobs to reach minimum load")
        
        # 提交补充作业
        submitted_jobs = []
        for _ in range(needed_jobs):
            job_id = self.submit_test_job(
    sleep_time=self.config["supplemental_sleep"],
    is_supplemental=1,
    load_ratio=None  # 补充作业不关联特定负载阶段
)
            if job_id:
                submitted_jobs.append(job_id)
                # 快速提交但控制节奏
                time.sleep(0.5)  # 防止短时间大量提交
        
        # 等待作业调度（最长2分钟）
        timeout = time.time() + 120
        while time.time() < timeout:
            allocated, total_nodes = self.get_queue_allocation()
            current_load = allocated / total_nodes
            
            if current_load >= target_load:
                print(f"Load achieved: {current_load:.1%}")
                return
            
            # 检查作业状态
            if submitted_jobs:
                try:
                    result = subprocess.run(
                        f"squeue -j {','.join(submitted_jobs)} -h -o '%T'",
                        shell=True,
                        capture_output=True,
                        text=True
                    )
                    states = result.stdout.split()
                    print(f"[Job Status] Submitted jobs states: {Counter(states)}")
                except Exception as e:
                    print(f"Status check error: {str(e)}")
            
            print(f"Current load {current_load:.1%}, retrying in 10 seconds...")
            time.sleep(10)
        
        print("Warning: Failed to reach target load within time limit")

    def cleanup_supplemental_jobs(self):
        """清理所有补充作业（包含已运行中的）"""
        print("\n[Cleanup] Starting cleanup of all supplemental jobs...")
        
        conn = sqlite3.connect(self.config["db_name"])
        try:
            cursor = conn.cursor()
            # 获取所有补充作业（无论是否已开始）
            cursor.execute('''
                SELECT job_id FROM job_metrics 
                WHERE is_supplemental = 1
            ''')
            all_supplemental = [row[0] for row in cursor.fetchall()]
            
            if not all_supplemental:
                print("No supplemental jobs found in database")
                return

            # 获取当前运行中的作业
            try:
                result = subprocess.run(
                    "squeue -h -o '%i'",
                    shell=True,
                    capture_output=True,
                    text=True
                )
                running_jobs = set(result.stdout.split())
            except Exception as e:
                print(f"Failed to get running jobs: {str(e)}")
                running_jobs = set()

            # 筛选需要取消的作业
            to_cancel = [jid for jid in all_supplemental if jid in running_jobs]
            print(f"Supplemental jobs to cancel: {len(to_cancel)}")
            
            # 批量取消作业
            success = 0
            for job_id in to_cancel:
                try:
                    subprocess.run(
                        f"scancel {job_id}",
                        shell=True,
                        check=True,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                    # 标记为已取消
                    cursor.execute('''
                        UPDATE job_metrics 
                        SET delay = -1 
                        WHERE job_id = ?
                    ''', (job_id,))
                    success += 1
                except Exception as e:
                    print(f"取消作业 {job_id} 失败: {str(e)}")
            
            conn.commit()
            print(f"Successfully canceled {success}/{len(to_cancel)} supplemental jobs")
            
        finally:
            conn.close()

    def run_benchmark(self, target_load):
        """Run benchmark test with enhanced load control"""
        # 确定下一阶段阈值
        load_levels = self.config["load_levels"]
        current_index = load_levels.index(target_load)
        next_index = current_index + 1
        max_allowed = load_levels[next_index] if next_index < len(load_levels) else 1.0
        buffer = self.config["buffer_threshold"]
        
        submission_interval = self.config["initial_interval"]
        consecutive_waits = 0  # 连续等待计数器
        
        for _ in range(self.config["num_jobs"]):
            # 获取队列资源状态
            allocated, total_nodes = self.get_queue_allocation()
            if total_nodes == 0:
                print("Error: No nodes available in the queue.")
                break
            
            # 计算当前和预估负载
            conn = sqlite3.connect(self.config["db_name"])
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM job_metrics 
                WHERE load_ratio = ? AND start_time IS NULL
            ''', (target_load,))
            pending_current = cursor.fetchone()[0]
            conn.close()
            
            current_load = allocated / total_nodes
            estimated_load = (allocated + pending_current) / total_nodes
            
            # 负载安全检测（考虑下一阶段阈值）
            safety_threshold = max_allowed - buffer
            if estimated_load >= safety_threshold:
                # 超过安全阈值时动态调整
                submission_interval = min(submission_interval * 2, self.config["max_interval"])
                consecutive_waits += 1
                print(f"Load approaching next stage: {estimated_load:.1%} ≥ {safety_threshold:.0%}, slowing down to {submission_interval:.2f}s")
                time.sleep(submission_interval)
                continue  # 跳过本次提交
            
            # 原始调控逻辑（目标负载调整）
            load_diff = current_load - target_load
            if load_diff < -0.05:  # 低于目标负载
                submission_interval *= self.config["adjust_rate"]
                consecutive_waits = 0
            elif load_diff > 0.05:  # 高于目标负载
                submission_interval *= (2 - self.config["adjust_rate"])
                consecutive_waits = 0
            
            # 应用间隔限制
            submission_interval = max(
                self.config["min_interval"], 
                min(submission_interval, self.config["max_interval"])
            )
            
            # 提交作业并记录
            job_id = self.submit_test_job(
    load_ratio=target_load,
    is_supplemental=0  # 明确标记为普通作业
)
            
            # 动态监控频率调整
            monitor_interval = max(1, submission_interval / 2)
            time.sleep(monitor_interval)
            self.monitor_jobs()
            
            # 如果连续等待次数过多，强制推进
            if consecutive_waits >= 5:
                print("Bypassing safety threshold after consecutive waits")
                consecutive_waits = 0
                submission_interval = self.config["initial_interval"]

    def generate_report(self):
        """Generate enhanced analysis report"""
        conn = sqlite3.connect(self.config["db_name"])
        try:
            df = pd.read_sql(
                "SELECT * FROM job_metrics WHERE start_time IS NOT NULL AND is_supplemental = 0", 
                conn
            )
        finally:
            conn.close()
        
        # 使用更专业的报告格式
        report = [
            "\n" + "="*80,
            f"{'Scheduling Performance Analysis Report':^80}",
            "="*80,
            f"\n{'Test Configuration':^80}\n" + "-"*80,
            f"Queue Name: {self.config['queue_name']}",
            f"Test Duration: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Test Jobs: {len(df):,}",
            "\n" + f"{'Performance Metrics':^80}\n" + "-"*80
        ]
        
        # 添加性能指标表格
        metrics = []
        for load in sorted(df['load_ratio'].unique()):
            subset = df[df['load_ratio'] == load]
            metrics.append([
                f"{load*100:.0f}%",
                f"{subset['delay'].mean():.2f}s",
                f"{subset['delay'].median():.2f}s",
                f"{subset['delay'].max():.2f}s",
                f"{subset['delay'].min():.2f}s",
                f"{len(subset):,}"
            ])
        
        # 使用tabulate库美化表格输出
        from tabulate import tabulate
        report.append(tabulate(
            metrics,
            headers=["Load Level", "Avg Delay", "Median Delay", "Max Delay", "Min Delay", "Job Count"],
            tablefmt="grid",
            floatfmt=".2f"
        ))
        
        # 添加总结部分
        report.extend([
            "\n" + f"{'Conclusion':^80}\n" + "-"*80,
            f"Overall Average Delay: {df['delay'].mean():.2f} seconds",
            f"95th Percentile Delay: {df['delay'].quantile(0.95):.2f} seconds",
            f"Standard Deviation: {df['delay'].std():.2f} seconds",
            "="*80
        ])
        
        print("\n".join(report))

    def generate_pdf_report(self):
        """Generate PDF report excluding supplemental jobs"""
        conn = sqlite3.connect(self.config["db_name"])
        try:
            # 只读取非补充作业的数据
            df = pd.read_sql('''
                SELECT * FROM job_metrics 
                WHERE is_supplemental = 0
            ''', conn)
            
            # 排除未完成的作业
            df_success = df[df['start_time'].notnull()]
        finally:
            conn.close()

        # 生成可视化（基于过滤后的数据）
        self.generate_visualization(df_success)

        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Helvetica", size=12)

        # PDF标题
        pdf.set_font_size(16)
        pdf.cell(0, 10, "Job Scheduling System Latency Test Report", 
                new_x=XPos.LMARGIN, new_y=YPos.NEXT, align='C')
        pdf.set_font_size(12)
        pdf.cell(0, 10, f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
            new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(10)

        # 汇总表格（只含普通作业）
        self.add_summary_table(pdf, df, df_success)
        
        # 可视化图表
        pdf.image("combined_visualization.png", x=10, w=190)
        pdf.ln(10)

        # 作业详情（自动过滤补充作业）
        self.add_job_details(pdf, df_success)

        # 保存报告
        report_path = "scheduling_benchmark_report.pdf"
        pdf.output(report_path)
        print(f"PDF report generated: {report_path}")

    def generate_visualization(self, df):
        """Generate professional visualization with updated style"""
        try:
            # 使用seaborn的默认样式（新版本使用'scatter'或'seaborn-v0_8'）
            plt.style.use('seaborn-v0_8') if 'seaborn-v0_8' in plt.style.available else plt.style.use('default')
        except:
            plt.style.use('default')  # 回退到默认样式
        
        plt.figure(figsize=(12, 10))
        
        # 创建子图布局
        gs = plt.GridSpec(2, 2, height_ratios=[1.5, 1], width_ratios=[1, 1])
        
        # 箱线图
        ax1 = plt.subplot(gs[0, :])
        sns.boxplot(
            x='load_ratio',
            y='delay',
            data=df,
            palette="Blues_d",
            width=0.6,
            showfliers=False,
            ax=ax1
        )
        ax1.set_title('Delay Distribution by Load Level', pad=15, fontsize=14)
        ax1.set_xlabel('Load Level', labelpad=10)
        ax1.set_ylabel('Delay (seconds)', labelpad=10)
        ax1.grid(True, alpha=0.3)
        
        # 折线图
        ax2 = plt.subplot(gs[1, 0])
        avg_delay = df.groupby('load_ratio')['delay'].mean().reset_index()
        sns.lineplot(
            x='load_ratio',
            y='delay',
            data=avg_delay,
            marker='o',
            markersize=8,
            linewidth=2.5,
            color='#FF7F0E',
            ax=ax2
        )
        ax2.set_title('Average Delay Trend', pad=15, fontsize=14)
        ax2.set_xlabel('Load Level', labelpad=10)
        ax2.set_ylabel('Average Delay (seconds)', labelpad=10)
        ax2.grid(True, alpha=0.3)
        
        # 直方图
        ax3 = plt.subplot(gs[1, 1])
        sns.histplot(
            df['delay'],
            bins=20,
            kde=True,
            color='#1f77b4',
            ax=ax3
        )
        ax3.set_title('Overall Delay Distribution', pad=15, fontsize=14)
        ax3.set_xlabel('Delay (seconds)', labelpad=10)
        ax3.set_ylabel('Frequency', labelpad=10)
        ax3.grid(True, alpha=0.3)
        
        # 调整布局
        plt.tight_layout(pad=3.0)
        plt.subplots_adjust(hspace=0.4, wspace=0.3)
        
        # 保存图片
        plt.savefig("combined_visualization.png", dpi=300, bbox_inches='tight')
        plt.close()

    def add_summary_table(self, pdf, df, df_success):
        """Add summary table to PDF"""
        pdf.set_font_size(14)
        pdf.cell(0, 10, "Test Summary", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font_size(12)
        
        # Calculate time range
        time_range = "N/A"
        if not df.empty:
            start_time = datetime.fromtimestamp(df['submit_time'].min()).strftime('%Y-%m-%d %H:%M:%S')
            end_time = datetime.fromtimestamp(df_success['start_time'].max()).strftime('%Y-%m-%d %H:%M:%S')
            time_range = f"{start_time} to {end_time}"

        # Create table
        col_widths = [60, 130]
        pdf.set_fill_color(200, 220, 255)
        
        rows = [
            ("Test Queue", self.config['queue_name']),
            ("Test Time Range", time_range),
            ("Total Submitted Jobs", str(len(df))),
            ("Completed Jobs", str(len(df_success)))
            #("Failed/Pending Jobs", str(len(df) - len(df_success)))
        ]

        # 添加各负载水平的统计信息
        load_levels = self.config["load_levels"]
        for load in load_levels:
            subset = df_success[df_success['load_ratio'] == load]
            if subset.empty:
                avg_delay_str = "N/A"
            else:
                avg_delay = subset['delay'].mean()
                avg_delay_str = f"{avg_delay:.2f}s"
            rows.append((f"{load*100:.0f}% Load Avg Delay", avg_delay_str))


        # 添加所有作业的平均时延
        if not df_success.empty:
            all_avg = df_success['delay'].mean()
            all_avg_str = f"{all_avg:.2f}s"
        else:
            all_avg_str = "N/A"
        rows.append(("All Jobs Average Delay", all_avg_str))

        for row in rows:
            pdf.cell(col_widths[0], 10, row[0], border=1, fill=True)
            pdf.cell(col_widths[1], 10, row[1], border=1)
            pdf.ln()
        
        pdf.ln(10)  

    def add_job_details(self, pdf, df):
        """Add filtered job details to PDF"""
        if df.empty:
            pdf.cell(0, 10, "No valid job records to display", 
                new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            return

        pdf.add_page()
        pdf.set_font_size(14)
        pdf.cell(0, 10, "Job Details", 
            new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_font_size(10)
        
        # 列定义（确保不显示补充作业字段）
        headers = ["Load Level", "Job ID", "Submit Time", "Start Time", "Delay(s)", "Retries"]
        columns = ['load_ratio', 'job_id', 'submit_time', 'start_time', 'delay', 'retries']
        col_widths = [25, 35, 40, 40, 25, 25]
        
        pdf.set_fill_color(200, 220, 255)
        for header, width in zip(headers, col_widths):
            pdf.cell(width, 10, header, border=1, fill=True)
        pdf.ln()

        fill = False
        for _, row in df.iterrows():
            if pdf.get_y() > 280:  # Page break check
                pdf.add_page()
                for header, width in zip(headers, col_widths):
                    pdf.cell(width, 10, header, border=1, fill=True)
                pdf.ln()
            
            submit_time = datetime.fromtimestamp(row['submit_time']).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['submit_time']) else "N/A"
            start_time = datetime.fromtimestamp(row['start_time']).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['start_time']) else "N/A"
            
            data = [
                f"{row['load_ratio']*100:.1f}%" if pd.notnull(row['load_ratio']) else "N/A",
                str(row['job_id']),
                submit_time,
                start_time,
                f"{row['delay']:.2f}" if pd.notnull(row['delay']) else "N/A",
                str(row['retries'])
            ]
            
            for value, width in zip(data, col_widths):
                pdf.cell(width, 10, value, border=1, fill=fill)
            pdf.ln()
            fill = not fill

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scheduler delay benchmarking tool')
    parser.add_argument('config', help='Path to JSON config file')
    args = parser.parse_args()
    
    benchmark = SchedulerBenchmark(args.config)
    try:
        # 获取当前实际负载
        allocated, total_nodes = benchmark.get_queue_allocation()
        if total_nodes > 0:
            current_load = allocated / total_nodes
        else:
            current_load = 0
        
        # Execute load tests (跳过已经超过的负载水平)
        for load_level in benchmark.config["load_levels"]:
            if current_load >= load_level:
                print(f"\n{'='*30} Skipping {load_level*100}% (current load {current_load*100:.1f}% already higher) {'='*30}")
                continue
                
            print(f"\n{'='*30} Testing load level: {load_level*100}% {'='*30}")
            benchmark.ensure_load_level(load_level)
            benchmark.run_benchmark(load_level)
            # Final status check
            for _ in range(benchmark.config["final_check_cycles"]):
                benchmark.monitor_jobs()
                time.sleep(benchmark.config["monitor_interval"])
    finally:
        benchmark.cleanup_supplemental_jobs()
        benchmark.generate_report()
        benchmark.generate_pdf_report()
