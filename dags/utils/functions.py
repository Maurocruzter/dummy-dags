from dateutil.parser import isoparse

def parse_ts_dag_run(
    dag_run_id: str,
    data_interval_end: str,
    external_trigger: bool,
    return_datetime: bool = False,
) -> str:

    if external_trigger:
        parsed = isoparse(dag_run_id.split("__")[-1])
    else:
        parsed = data_interval_end

    if return_datetime:
        return parsed

    return parsed.strftime("%Y%m%dT%H%M%S")