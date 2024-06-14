from datetime import datetime

from airflow import settings
from airflow.models import TaskInstance
from airflow.utils.state import State
from sqlalchemy.orm import sessionmaker


def last_successful(dag_id, task_id, use_task_id=True):
    Session = sessionmaker(bind=settings.engine)
    session = Session()
    last_task_instance = (
        session.query(TaskInstance)
        .filter(TaskInstance.dag_id == dag_id)
        .filter(TaskInstance.task_id == task_id)
        .filter(TaskInstance.state == State.SUCCESS)
        if use_task_id
        else session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id).filter(
            TaskInstance.state == State.SUCCESS)
    )
    last_task_instances = last_task_instance.all()
    session.close()
    if last_task_instances:
        mx = max(ti.end_date for ti in last_task_instances)
        mx = datetime(mx.year, mx.month, mx.day)
        return mx
    return datetime(1, 1, 1)
