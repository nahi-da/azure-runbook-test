#!/usr/bin/env python3
"""stop_vm_runbook_v2.py

Azure VM停止Runbook
- VMを監視しているアラートルールを無効化
- アラート無効化を確認
- VMを停止

使い方:
    python stop_vm_runbook_v2.py

設定:
    VM_ALERT_CONFIG: VMとアラートルールのマッピング
    RETRY_CONFIG: リトライ設定
"""

from __future__ import annotations

import logging
import sys
import time
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from functools import wraps

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient


# ============================================================================
# 設定セクション
# ============================================================================

# タイムゾーン
JST = timezone(timedelta(hours=9))

# VM停止対象とアラートルールの定義
# キー: VMのリソースID
# 値: アラートルールのリソースIDのリスト
VM_ALERT_CONFIG = {
    # 例: VM 1台とそれに関連するアラートルール2件
    "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.compute/virtualMachines/your-vm-name": [
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/scheduledqueryrules/your-alert-rule-1",
        "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/activityLogAlerts/your-alert-rule-2",
    ],
    # 追加のVMがあればここに記載
    # "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.compute/virtualMachines/your-vm-name-2": [
    #     "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/your-resource-group/providers/microsoft.insights/metricalerts/your-alert-rule-3",
    # ],
}

# リトライ設定
RETRY_CONFIG = {
    "max_retries": 3,   # 最大リトライ回数
    "retry_delay": 10,  # リトライ間隔（秒）
}

# アラートタイプ → MonitorManagementClient の属性名マッピング
_ALERT_TYPE_MAP = {
    "microsoft.insights/metricalerts":        "metric_alerts",
    "microsoft.insights/scheduledqueryrules": "scheduled_query_rules",
    "microsoft.insights/activitylogalerts":   "activity_log_alerts",
}


# ============================================================================
# ログ設定
# ============================================================================

def setup_logging() -> logging.Logger:
    logging.getLogger("azure").setLevel(logging.WARNING)

    formatter = logging.Formatter(fmt="%(levelname)s: %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger


# ============================================================================
# ユーティリティ関数
# ============================================================================

def parse_resource_id(resource_id: str) -> Dict[str, str]:
    """
    Azure リソースIDをパースしてコンポーネントを抽出

    例: /subscriptions/{sub}/resourceGroups/{rg}/providers/{provider}/{type}/{name}
    """
    pattern = (
        r"/subscriptions/(?P<subscription>[^/]+)"
        r"/resourceGroups/(?P<resource_group>[^/]+)"
        r"/providers/(?P<provider>[^/]+)"
        r"/(?P<resource_type>[^/]+)"
        r"/(?P<resource_name>[^/]+)"
    )
    match = re.match(pattern, resource_id, re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid Azure resource ID format: {resource_id}")
    return match.groupdict()


def retry(logger: logging.Logger, config: Dict[str, Any]):
    """リトライデコレータ（固定間隔）"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            max_retries = config.get("max_retries", 3)
            delay = config.get("retry_delay", 10)
            for attempt in range(1, max_retries + 2):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt > max_retries:
                        logger.error(f"{func.__name__} が {max_retries} 回のリトライ後に失敗: {e}")
                        raise
                    logger.warning(f"{func.__name__} 失敗 (試行 {attempt}/{max_retries}): {e}")
                    time.sleep(delay)
        return wrapper
    return decorator


# ============================================================================
# Azure操作クラス
# ============================================================================

class AzureRunbookOperations:
    """Azure VM停止Runbook操作クラス"""

    def __init__(self, credential: DefaultAzureCredential, logger: logging.Logger, retry_config: Dict[str, Any]):
        self.credential = credential
        self.logger = logger
        self.retry_config = retry_config
        self._compute_clients: Dict[str, ComputeManagementClient] = {}
        self._monitor_clients: Dict[str, MonitorManagementClient] = {}

    def _compute(self, subscription_id: str) -> ComputeManagementClient:
        """ComputeManagementClientをサブスクリプション単位でキャッシュして返す"""
        if subscription_id not in self._compute_clients:
            self._compute_clients[subscription_id] = ComputeManagementClient(self.credential, subscription_id)
        return self._compute_clients[subscription_id]

    def _monitor(self, subscription_id: str) -> MonitorManagementClient:
        """MonitorManagementClientをサブスクリプション単位でキャッシュして返す"""
        if subscription_id not in self._monitor_clients:
            self._monitor_clients[subscription_id] = MonitorManagementClient(self.credential, subscription_id)
        return self._monitor_clients[subscription_id]

    def _alert_ops(self, alert_resource_id: str):
        """
        アラートリソースIDからMonitorクライアントの操作オブジェクトとパース結果を返す

        Returns:
            tuple: (操作オブジェクト, parse_resource_id の返り値)
        """
        p = parse_resource_id(alert_resource_id)
        full_type = f"{p['provider']}/{p['resource_type']}".lower()
        attr = _ALERT_TYPE_MAP.get(full_type)
        if attr is None:
            raise ValueError(f"サポートされていないアラートルールの種類です: {full_type}")
        return getattr(self._monitor(p["subscription"]), attr), p

    def disable_alert_rule(self, alert_resource_id: str) -> None:
        """アラートルールを無効化（種類を自動判別）"""
        ops, p = self._alert_ops(alert_resource_id)
        name, rg = p["resource_name"], p["resource_group"]

        @retry(self.logger, self.retry_config)
        def _disable():
            resource = ops.get(rg, name)
            resource.enabled = False
            ops.create_or_update(rg, name, resource)
            print("    -> OK")

        _disable()

    def verify_alert_rule_disabled(self, alert_resource_id: str) -> bool:
        """アラートルールが無効化されているか確認（種類を自動判別）"""
        ops, p = self._alert_ops(alert_resource_id)
        name, rg = p["resource_name"], p["resource_group"]

        resource = ops.get(rg, name)
        is_disabled = not resource.enabled

        if is_disabled:
            print("    -> OK")
        else:
            self.logger.error(f"  まだ有効です: {name}")
        return is_disabled

    def stop_vm(self, vm_resource_id: str) -> None:
        """VMを停止（割り当て解除）"""
        p = parse_resource_id(vm_resource_id)
        rg, vm_name = p["resource_group"], p["resource_name"]

        @retry(self.logger, self.retry_config)
        def _stop():
            poller = self._compute(p["subscription"]).virtual_machines.begin_deallocate(rg, vm_name)
            poller.result()
            print("    -> OK")

        _stop()

    def verify_vm_stopped(self, vm_resource_id: str) -> bool:
        """VMが割り当て解除状態か確認"""
        p = parse_resource_id(vm_resource_id)
        rg, vm_name = p["resource_group"], p["resource_name"]

        iv = self._compute(p["subscription"]).virtual_machines.instance_view(rg, vm_name)

        power_state = next(
            (s.code for s in (iv.statuses or []) if s.code and s.code.startswith("PowerState/")),
            None
        )
        if not power_state:
            self.logger.error(f"VMのPowerStateを取得できませんでした: {vm_name}")
            return False

        print(f"    - 状態: {power_state}")
        if power_state == "PowerState/deallocated":
            print("    -> OK")
            return True
        else:
            self.logger.error(f"確認失敗: VMが割り当て解除状態ではありません: {vm_name} ({power_state})")
            return False


# ============================================================================
# メイン処理
# ============================================================================

def _set_alert_failed(alerts: list, name: str, error: str) -> None:
    """アラート結果リストの該当エントリをfailedに更新する"""
    for alert in alerts:
        if alert["name"] == name:
            alert["status"] = "failed"
            alert["error"] = error
            break


def print_execution_summary(execution_results: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    実行結果サマリを出力

    Returns:
        bool: 全て成功した場合True、失敗があった場合False
    """
    print("")
    print("=" * 80)
    print("VM停止Runbook 実行結果サマリ")
    print("=" * 80)

    start_time = execution_results["start_time"]
    end_time = execution_results["end_time"]
    print(f"実行開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')} JST")
    print(f"実行終了: {end_time.strftime('%Y-%m-%d %H:%M:%S')} JST")
    print(f"総処理時間: {str(end_time - start_time).split('.')[0]}")
    print("")

    total_vms = len(execution_results["vms"])
    success_count = failed_count = 0
    total_alerts = success_alerts = failed_alerts = 0

    for idx, vm_result in enumerate(execution_results["vms"], 1):
        print("-" * 80)
        print(f"【VM {idx}/{total_vms}: {vm_result['vm_name']}】")
        print("-" * 80)

        print("  アラートルール無効化:")
        for alert in vm_result["alerts"]:
            total_alerts += 1
            if alert["status"] == "success":
                success_alerts += 1
                print(f"    ✓ {alert['name']:<30s}: 無効化完了")
            else:
                failed_alerts += 1
                print(f"    ✗ {alert['name']:<30s}: 無効化失敗 ({alert['error']})")

        print("")

        print("  VM割り当て解除:")
        vm_stop = vm_result["vm_stop"]
        if vm_stop["status"] == "success":
            print(f"    ✓ 実行完了 ({vm_stop['power_state']})")
        elif vm_stop["status"] == "skipped":
            print(f"    ⊘ スキップ ({vm_stop['error']})")
        else:
            print(f"    ✗ 失敗 ({vm_stop['error']})")

        print("")

        overall = vm_result["overall_status"]
        vm_duration = vm_result["end_time"] - vm_result["start_time"]
        icon = "✓" if overall == "success" else "✗"
        text = "成功" if overall == "success" else "失敗"
        print(f"  処理結果: {icon} {text}")
        print(f"  処理時間: {str(vm_duration).split('.')[0]}")
        print("")

        if overall == "success":
            success_count += 1
        else:
            failed_count += 1

    success_vms = [vm["vm_name"] for vm in execution_results["vms"] if vm["overall_status"] == "success"]
    failed_vms  = [vm["vm_name"] for vm in execution_results["vms"] if vm["overall_status"] == "failed"]

    print("=" * 80)
    print("【全体サマリ】")
    print(f"  対象VM数: {total_vms}台")
    print(f"  成功: {success_count}台" + (f" ({', '.join(success_vms)})" if success_vms else ""))
    print(f"  失敗: {failed_count}台" + (f" ({', '.join(failed_vms)})" if failed_vms else ""))
    print("")
    print(f"  総アラートルール数: {total_alerts}件")
    print(f"  無効化成功: {success_alerts}件")
    print(f"  無効化失敗: {failed_alerts}件")
    print("")
    print("  実行結果: " + ("✓ 全て成功" if failed_count == 0 else "✗ 一部または全て失敗"))
    print("=" * 80)

    return failed_count == 0


def main():
    """VM停止Runbookのメイン処理"""
    starttime = datetime.now(JST)
    logger = setup_logging()
    print("=" * 80)
    print("VM停止Runbook開始", starttime)
    print("=" * 80)

    execution_results = {
        "start_time": starttime,
        "end_time": None,
        "vms": []
    }

    try:
        print("認証開始")
        credential = DefaultAzureCredential()
        operations = AzureRunbookOperations(credential, logger, RETRY_CONFIG)
        print("-> 認証完了")
        print("")

        total_vms = len(VM_ALERT_CONFIG)
        print("監視抑止・VM割り当て解除開始")
        for idx, (vm_resource_id, alert_rules) in enumerate(VM_ALERT_CONFIG.items(), 1):
            vm_name = parse_resource_id(vm_resource_id)["resource_name"]
            vm_result = {
                "vm_name": vm_name,
                "start_time": datetime.now(JST),
                "end_time": None,
                "alerts": [],
                "vm_stop": {"status": "skipped", "power_state": None, "error": "未実行"},
                "overall_status": "failed"
            }

            print(f"[{idx}/{total_vms}]: {vm_name}")

            try:
                # ステップ1: アラートルール無効化
                print("  - アラートルール無効化 {} 件".format(len(alert_rules)))
                for i, alert_id in enumerate(alert_rules, 1):
                    alert_name = parse_resource_id(alert_id)["resource_name"]
                    print("    - [{}/{}] {}".format(i, len(alert_rules), alert_name))
                    try:
                        operations.disable_alert_rule(alert_id)
                        vm_result["alerts"].append({"name": alert_name, "status": "success", "error": None})
                    except Exception as e:
                        vm_result["alerts"].append({"name": alert_name, "status": "failed", "error": str(e).split('\n')[0]})
                        logger.error(f"  アラートルールの無効化に失敗: {alert_name}")
                        logger.exception(e)
                        raise # アラート無効化失敗時はVM停止をスキップ

                # ステップ2: アラート無効化確認
                print("  - アラートルール状態確認 {} 件".format(len(alert_rules)))
                all_disabled = True
                for i, alert_id in enumerate(alert_rules, 1):
                    alert_name = parse_resource_id(alert_id)["resource_name"]
                    print("    - [{}/{}] {}".format(i, len(alert_rules), alert_name))
                    try:
                        if not operations.verify_alert_rule_disabled(alert_id):
                            all_disabled = False
                            _set_alert_failed(vm_result["alerts"], alert_name, "無効化確認に失敗")
                    except Exception as e:
                        all_disabled = False
                        logger.error(f"  確認エラー: {alert_name}")
                        logger.exception(e)
                        _set_alert_failed(vm_result["alerts"], alert_name, "確認エラー")

                if not all_disabled:
                    vm_result["vm_stop"]["error"] = "アラート無効化に失敗したため"
                    logger.error(f"一部のアラートルールが有効です。VM停止をスキップします: {vm_name}")
                    continue

                print("  -> アラートルール無効化完了")
                print("")

                # ステップ3: VM停止
                print("  - VM割り当て解除要求送信")
                operations.stop_vm(vm_resource_id)

                # ステップ4: VM停止確認
                print("  - VM割り当て解除が成功したか確認")
                if not operations.verify_vm_stopped(vm_resource_id):
                    vm_result["vm_stop"]["status"] = "failed"
                    vm_result["vm_stop"]["error"] = "停止状態の確認に失敗"
                    raise Exception(f"VM {vm_name} が停止状態ではありません")

                # verify_vm_stopped が True を返した時点で deallocated 確定
                vm_result["vm_stop"]["status"] = "success"
                vm_result["vm_stop"]["power_state"] = "PowerState/deallocated"
                vm_result["vm_stop"]["error"] = None
                vm_result["overall_status"] = "success"
                print("  -> OK")
                print("")

            except Exception as e:
                logger.error(f"{vm_name} の処理に失敗: {e}")
                logger.exception(e)
                vm_result["overall_status"] = "failed"
            finally:
                vm_result["end_time"] = datetime.now(JST)
                execution_results["vms"].append(vm_result)

        execution_results["end_time"] = datetime.now(JST)

        if not print_execution_summary(execution_results, logger):
            raise Exception("VM停止処理の異常を検知しました。")

    except Exception as e:
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
