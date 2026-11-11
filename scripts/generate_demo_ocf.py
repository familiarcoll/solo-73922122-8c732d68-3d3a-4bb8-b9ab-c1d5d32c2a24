"""生成递归故障演示 OCF 归档(scripts 目录,非服务运行时代码)。

用法:
    python3 -I scripts/generate_demo_ocf.py examples/fault_events_demo.ocf [codec]

数据按 demo.py 的 writer 模式(FaultEvent/Fault,cause 递归引用)构造,
切成 3 个 block,默认 deflate 压缩,便于演示分块导入、坏块不推进与续导。
"""

from __future__ import annotations

import sys

from app.avro_codec import write_ocf
from app.avro_schema import parse
from app.demo import DEMO_RECURSIVE_REQUEST

WRITER_SCHEMA = DEMO_RECURSIVE_REQUEST["writer"]

# 4 条故障事件,故意覆盖:深层递归 cause、null cause、长 tags、多字节字符串
RECORDS = [
    {
        "eventId": "evt-0001",
        "vehicleId": "VIN-沪A-00001",
        "severity": 3,
        "fault": {
            "code": "BATT_OVERHEAT",
            "message": "动力电池温度超限",
            "cause": {
                "code": "COOLANT_LOW",
                "message": "冷却液液位低",
                "cause": {
                    "code": "PUMP_FAULT",
                    "message": None,
                    "cause": None,
                    "tags": ["powertrain", "cooling", "pump"],
                },
                "tags": ["powertrain", "cooling"],
            },
            "tags": ["battery", "thermal"],
        },
    },
    {
        "eventId": "evt-0002",
        "vehicleId": "VIN-京B-00042",
        "severity": 1,
        "fault": {
            "code": "TPMS_WARN",
            "message": None,
            "cause": None,
            "tags": ["chassis", "tire"],
        },
    },
    {
        "eventId": "evt-0003",
        "vehicleId": "VIN-粤C-00108",
        "severity": 2,
        "fault": {
            "code": "CAN_BUS_OFF",
            "message": "CAN 总线关闭",
            "cause": {
                "code": "ECU_TIMEOUT",
                "message": "网关 ECU 应答超时",
                "cause": None,
                "tags": ["network", "gateway"],
            },
            "tags": ["network", "can", "bus-off"],
        },
    },
    {
        "eventId": "evt-0004",
        "vehicleId": "VIN-川D-00256",
        "severity": 4,
        "fault": {
            "code": "BRAKE_ABS_FAULT",
            "message": "ABS 防抱死失效",
            "cause": {
                "code": "WHEEL_SENSOR_LOSS",
                "message": "左后轮速信号丢失",
                "cause": {
                    "code": "HARNESS_OPEN",
                    "message": "传感器线束开路",
                    "cause": None,
                    "tags": ["chassis", "wiring"],
                },
                "tags": ["chassis", "brake", "sensor"],
            },
            "tags": ["chassis", "brake", "critical"],
        },
    },
]

# 每个子列表是一个 block:2 + 1 + 1,共 3 块
BLOCKS = [RECORDS[0:2], RECORDS[2:3], RECORDS[3:4]]


def main(argv: list[str]) -> int:
    out_path = argv[1] if len(argv) > 1 else "examples/fault_events_demo.ocf"
    codec = argv[2] if len(argv) > 2 else "deflate"
    if codec not in ("null", "deflate"):
        print(f"不支持的编码 {codec}", file=sys.stderr)
        return 2
    schema_node = parse(WRITER_SCHEMA)
    with open(out_path, "wb") as f:
        write_ocf(f, WRITER_SCHEMA, BLOCKS, schema_node, codec=codec)
    print(f"已生成 {out_path}(codec={codec},blocks={len(BLOCKS)},"
          f"records={len(RECORDS)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
