"""递归模式演示请求体:车载故障消息(兼容示例)。

场景:故障事件 FaultEvent 内含 Fault,Fault 通过 cause 字段递归引用自身
(故障级联:一个故障可能由另一个故障引起)。reader 演进:
- severity 由 int 提升为 long(允许的提升);
- 忽略旧字段 fault.message(writer 字段可被忽略);
- 新增字段 occurredAt、fault.component,均带类型相符的默认值;
- fault.cause 的 reader union 增加 string 分支(writer 每分支仍可读)。
"""

DEMO_RECURSIVE_REQUEST = {
    "writer": {
        "type": "record",
        "name": "FaultEvent",
        "fields": [
            {"name": "eventId", "type": "string"},
            {"name": "vehicleId", "type": "string"},
            {"name": "severity", "type": "int"},
            {
                "name": "fault",
                "type": {
                    "type": "record",
                    "name": "Fault",
                    "fields": [
                        {"name": "code", "type": "string"},
                        {
                            "name": "message",
                            "type": ["null", "string"],
                            "default": None,
                        },
                        {
                            "name": "cause",
                            "type": ["null", "Fault"],
                            "default": None,
                        },
                        {
                            "name": "tags",
                            "type": {"type": "array", "items": "string"},
                        },
                    ],
                },
            },
        ],
    },
    "reader": {
        "type": "record",
        "name": "FaultEvent",
        "fields": [
            {"name": "eventId", "type": "string"},
            {"name": "vehicleId", "type": "string"},
            {"name": "severity", "type": "long"},
            {
                "name": "occurredAt",
                "type": "long",
                "default": 0,
            },
            {
                "name": "fault",
                "type": {
                    "type": "record",
                    "name": "Fault",
                    "fields": [
                        {"name": "code", "type": "string"},
                        {
                            "name": "component",
                            "type": "string",
                            "default": "unknown",
                        },
                        {
                            "name": "cause",
                            "type": ["null", "Fault", "string"],
                            "default": None,
                        },
                        {
                            "name": "tags",
                            "type": {"type": "array", "items": "string"},
                        },
                    ],
                },
            },
        ],
    },
}
