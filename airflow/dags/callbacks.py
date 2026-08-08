"""
airflow/dags/callbacks.py

Gửi thông báo Slack khi các task quan trọng trong pipeline
thành công hoặc thất bại.

Cơ chế: Airflow tự động gọi on_failure_callback / on_success_callback
với đúng 1 tham số — dict `context` — mỗi khi task đổi trạng thái.
context['exception'] CHỈ tồn tại ở failure callback, không có ở success
callback — đây là lý do tách 2 hàm riêng thay vì dùng chung 1 hàm.
"""
import logging

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _get_period_str(context) -> str:
    """
    Lấy tháng dữ liệu THẬT đang xử lý — đọc từ XCom của get_target_period,
    KHÔNG dùng logical_date hay data_interval_start.

    Lý do: pipeline này có business logic lệch 2 tháng so với lịch chạy
    (TLC data trễ ~2 tháng). data_interval_start chỉ phản ánh khung lịch
    schedule của Airflow, không phản ánh đúng tháng dữ liệu TLC thật sự
    được xử lý trong run này — nguồn đáng tin cậy duy nhất là giá trị
    year_month mà get_target_period() đã tính và trả về qua XCom.
    """
    ti = context["task_instance"]
    try:
        period = ti.xcom_pull(task_ids="get_target_period")
        if period:
            return period["year_month"]
    except Exception:
        pass
    interval_start = context["dag_run"].data_interval_start
    return f"{interval_start.strftime('%Y-%m')} (ước tính theo lịch chạy)"


def _post_to_slack(message: str) -> None:
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL chưa được set — bỏ qua gửi thông báo.")
        return
    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Gửi Slack thất bại: {e}")


def notify_failure(context) -> None:
    ti = context["task_instance"]
    period_str = _get_period_str(context)
    exception = context.get("exception")

    message = (
        f":x: *NYC Taxi Pipeline — Task FAILED*\n"
        f"*DAG:* `{ti.dag_id}`\n"
        f"*Task:* `{ti.task_id}`\n"
        f"*Tháng dữ liệu:* `{period_str}`\n"
        f"*Lỗi:* `{exception}`\n"
        f"*Log:* <{ti.log_url}|Xem chi tiết>"
    )
    _post_to_slack(message)


def notify_success(context) -> None:
    ti = context["task_instance"]
    period_str = _get_period_str(context)

    message = (
        f":white_check_mark: *NYC Taxi Pipeline — Hoàn tất thành công*\n"
        f"*DAG:* `{ti.dag_id}`\n"
        f"*Tháng dữ liệu:* `{period_str}`\n"
        f"*Log:* <{ti.log_url}|Xem chi tiết>"
    )
    _post_to_slack(message)