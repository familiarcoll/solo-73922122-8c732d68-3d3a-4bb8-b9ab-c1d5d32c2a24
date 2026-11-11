"""Avro 模式子集的解析与校验。

支持范围:null、boolean、int、long、string、array、具名 record、union,
允许递归引用(通过具名 record),不支持 namespace 与 aliases。

解析结果是一棵节点树;递归引用通过共享 Record 节点对象实现,
因此兼容检查可以用 (writer 节点 id, reader 节点 id) 对做环检测。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

PRIMITIVES = ("null", "boolean", "int", "long", "string")

INT_MIN, INT_MAX = -(2**31), 2**31 - 1
LONG_MIN, LONG_MAX = -(2**63), 2**63 - 1

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SchemaError(Exception):
    """非法模式错误,path 为出错位置(基于原始 JSON 结构的路径)。"""

    def __init__(self, path: str, message: str):
        super().__init__(f"{path}: {message}")
        self.path = path
        self.message = message


# ---------------------------------------------------------------- 节点定义

@dataclass(eq=False)
class Primitive:
    name: str  # null / boolean / int / long / string


@dataclass(eq=False)
class Array:
    items: "Node"


@dataclass(eq=False)
class Field:
    name: str
    type: "Node"
    has_default: bool
    default: Any = None


@dataclass(eq=False)
class Record:
    name: str
    fields: List[Field] = field(default_factory=list)


@dataclass(eq=False)
class Union:
    branches: List["Node"]


Node = Any  # Primitive | Array | Record | Union


def describe(node: Node) -> str:
    """节点的可读描述,用于报错信息。"""
    if isinstance(node, Primitive):
        return node.name
    if isinstance(node, Array):
        return "array"
    if isinstance(node, Record):
        return f"record {node.name}"
    if isinstance(node, Union):
        return "union[" + ", ".join(describe(b) for b in node.branches) + "]"
    return str(type(node).__name__)


# ---------------------------------------------------------------- 解析

def parse(doc: Any) -> Node:
    """把 JSON 文档解析为模式节点树,非法时抛出 SchemaError(带路径)。"""
    return _parse(doc, "$", {})


def _parse(node: Any, path: str, symbols: Dict[str, Record]) -> Node:
    # 1) 字符串:基础类型或已定义 record 的引用(递归引用由此解析)
    if isinstance(node, str):
        if node in PRIMITIVES:
            return Primitive(node)
        if node in symbols:
            return symbols[node]
        raise SchemaError(path, f"未知类型名 '{node}'(既不是基础类型,也不是已定义的 record)")

    # 2) JSON 数组:union
    if isinstance(node, list):
        if not node:
            raise SchemaError(path, "union 至少需要一个分支")
        branches = [
            _parse(b, f"{path}[{i}]", symbols) for i, b in enumerate(node)
        ]
        for i, b in enumerate(branches):
            if isinstance(b, Union):
                raise SchemaError(f"{path}[{i}]", "不允许 union 直接嵌套 union")
        seen = set()
        for i, b in enumerate(branches):
            key = _branch_key(b)
            if key in seen:
                raise SchemaError(f"{path}[{i}]", f"union 分支重复:{describe(b)}")
            seen.add(key)
        return Union(branches)

    # 3) JSON 对象:{"type": ...} 形式
    if isinstance(node, dict):
        if "namespace" in node:
            raise SchemaError(f"{path}.namespace", "不支持 namespace(本服务范围不含命名空间)")
        if "aliases" in node:
            raise SchemaError(f"{path}.aliases", "不支持 aliases(本服务范围不含别名)")
        if "type" not in node:
            raise SchemaError(path, "缺少 'type' 属性")
        t = node["type"]
        # {"type": <对象或数组>} 的简写形式
        if isinstance(t, (dict, list)):
            return _parse(t, f"{path}.type", symbols)
        if not isinstance(t, str):
            raise SchemaError(f"{path}.type", "'type' 必须是字符串、对象或数组")
        if t in PRIMITIVES:
            return Primitive(t)
        if t == "array":
            if "items" not in node:
                raise SchemaError(path, "array 类型缺少 'items' 属性")
            return Array(_parse(node["items"], f"{path}.items", symbols))
        if t == "record":
            return _parse_record(node, path, symbols)
        # {"type": "SomeRecord"}:对具名 record 的引用
        if t in symbols:
            return symbols[t]
        raise SchemaError(
            f"{path}.type",
            f"未知类型 '{t}'(支持 {', '.join(PRIMITIVES)}、array、record 或已定义的 record 名)",
        )

    raise SchemaError(path, f"非法模式节点:JSON 类型 {type(node).__name__} 不是合法的模式")


def _branch_key(node: Node) -> str:
    if isinstance(node, Record):
        return f"record:{node.name}"
    return describe(node)


def _parse_record(node: dict, path: str, symbols: Dict[str, Record]) -> Record:
    name = node.get("name")
    if not isinstance(name, str) or not _NAME_RE.match(name):
        raise SchemaError(
            f"{path}.name",
            "record 缺少合法名称(须匹配 [A-Za-z_][A-Za-z0-9_]*)",
        )
    if name in symbols:
        raise SchemaError(f"{path}.name", f"record 名称 '{name}' 重复定义")
    if name in PRIMITIVES or name in ("array", "record"):
        raise SchemaError(f"{path}.name", f"record 名称 '{name}' 与保留类型名冲突")

    # 先注册再解析字段:字段内可以用名称引用自身,实现递归模式
    rec = Record(name)
    symbols[name] = rec

    fields_json = node.get("fields")
    if not isinstance(fields_json, list):
        raise SchemaError(f"{path}.fields", "record 缺少 'fields' 数组")

    seen_fields = set()
    for i, fj in enumerate(fields_json):
        fpath = f"{path}.fields[{i}]"
        if not isinstance(fj, dict):
            raise SchemaError(fpath, "字段定义必须是对象")
        fname = fj.get("name")
        if not isinstance(fname, str) or not _NAME_RE.match(fname):
            raise SchemaError(f"{fpath}.name", "字段缺少合法名称")
        if fname in seen_fields:
            raise SchemaError(f"{fpath}.name", f"字段名 '{fname}' 重复")
        seen_fields.add(fname)
        if "type" not in fj:
            raise SchemaError(fpath, f"字段 '{fname}' 缺少 'type'")
        ftype = _parse(fj["type"], f"{fpath}.type", symbols)
        has_default = "default" in fj
        rec.fields.append(Field(fname, ftype, has_default, fj.get("default")))

    return rec
