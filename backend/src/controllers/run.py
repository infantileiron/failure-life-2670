from src.services import run as service
from src.utils import repository as repo


def list_items(): return service.list_runs()


def detail(id): return service.detail(id)


def create(data): return service.create(data)


def retry(id):
    """Manually requeue a failed run under its existing id."""
    run = service.detail(id)
    if run["status"] == "completed":
        raise ValueError("已完成的运行不需要重试")
    if run["status"] == "running":
        raise ValueError("运行正在计算中")
    updated = service.request_retry(id)
    if not updated:
        raise ValueError("运行已达到最大重试次数，请重新提交")
    return updated
