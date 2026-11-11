"""静态兼容检查:判断 reader 模式能否读取 writer 模式写出的全部消息。

规则(仅覆盖本服务支持的模式子集):
- 基础类型必须相同,唯一提升:int -> long;
- record 名称必须相同,字段按名称匹配;writer 有而 reader 没有的字段被忽略;
  reader 新增字段必须带类型相符的默认值(union 类型的默认值按首分支校验);
- 数组对数组递归检查 items;
- union:writer 的每个分支都必须能被 reader 的至少一个分支读取
  (writer 非 union 视为单分支;reader 非 union 视为单分支);
- 递归 record 不能只凭名称相同判定兼容:首次遇到时展开字段逐一检查,
  用"正在比较中的 (writer, reader) 节点对"集合终止递归。

不兼容时返回第一处问题的路径与原因。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Set, Tuple

from .avro_schema import (
    INT_MIN,
    INT_MAX,
    LONG_MIN,
    LONG_MAX,
    Array,
    Node,
    Primitive,
    Record,
    Union,
    describe,
)


@dataclass
class Issue:
    path: str
    reason: str


def check_compatibility(writer: Node, reader: Node) -> Optional[Issue]:
    """返回 None 表示兼容,否则返回第一处不兼容的位置与原因。"""
    return _check(writer, reader, "$", set())


def _check(
    w: Node, r: Node, path: str, in_progress: Set[Tuple[int, int]]
) -> Optional[Issue]:
    # ---- 基础类型 ----
    if isinstance(w, Primitive) and isinstance(r, Primitive):
        if w.name == r.name:
            return None
        if w.name == "int" and r.name == "long":
            return None
        return Issue(
            path,
            f"基础类型不匹配:writer 为 '{w.name}',reader 为 '{r.name}'"
            "(仅允许 int 提升为 long,其余基础类型必须相同)",
        )

    # ---- record ----
    if isinstance(w, Record) and isinstance(r, Record):
        if w.name != r.name:
            return Issue(
                path,
                f"record 名称不匹配:writer 为 '{w.name}',reader 为 '{r.name}'",
            )
        key = (id(w), id(r))
        if key in in_progress:
            # 递归引用:该对节点正在向上展开比较,此处不再展开,
            # 兼容与否由外层正在进行的字段级比较决定。
            return None
        in_progress.add(key)
        try:
            w_fields = {f.name: f for f in w.fields}
            for rf in r.fields:
                fpath = f"{path}.fields.{rf.name}"
                wf = w_fields.get(rf.name)
                if wf is not None:
                    issue = _check(wf.type, rf.type, fpath, in_progress)
                    if issue is not None:
                        return issue
                else:
                    # reader 新增字段:必须有类型相符的默认值
                    if not rf.has_default:
                        return Issue(
                            fpath,
                            f"reader 新增字段 '{rf.name}' 在 writer 中不存在,"
                            "且没有默认值,无法读取旧消息",
                        )
                    issue = validate_default(
                        rf.default, rf.type, f"{fpath}.default"
                    )
                    if issue is not None:
                        return issue
            # writer 有而 reader 没有的字段:允许,被忽略
            return None
        finally:
            in_progress.discard(key)

    # ---- 数组 ----
    if isinstance(w, Array) and isinstance(r, Array):
        return _check(w.items, r.items, f"{path}.items", in_progress)

    # ---- union(任一侧为 union 时在此处理)----
    if isinstance(w, Union) or isinstance(r, Union):
        w_branches: List[Node] = w.branches if isinstance(w, Union) else [w]
        r_branches: List[Node] = r.branches if isinstance(r, Union) else [r]
        for i, wb in enumerate(w_branches):
            bpath = f"{path}[{i}]" if isinstance(w, Union) else path
            first_err: Optional[Issue] = None
            readable = False
            for rb in r_branches:
                err = _check(wb, rb, bpath, in_progress)
                if err is None:
                    readable = True
                    break
                if first_err is None:
                    first_err = err
            if not readable:
                detail = (
                    f";例如与 reader 首分支比较:{first_err.reason}"
                    f"(位于 {first_err.path})"
                    if first_err is not None
                    else ""
                )
                return Issue(
                    bpath,
                    f"writer 分支 {i}({describe(wb)})无法被 reader 的任何分支读取"
                    + detail,
                )
        return None

    # ---- 类别不同 ----
    return Issue(
        path,
        f"类型类别不匹配:writer 为 {describe(w)},reader 为 {describe(r)}",
    )


# ---------------------------------------------------------------- 默认值校验

def validate_default(value: Any, node: Node, path: str) -> Optional[Issue]:
    """校验默认值与类型是否相符;union 类型按首分支校验。"""
    if isinstance(node, Union):
        return validate_default(value, node.branches[0], f"{path}[0]")

    if isinstance(node, Primitive):
        return _validate_primitive_default(value, node.name, path)

    if isinstance(node, Array):
        if not isinstance(value, list):
            return Issue(path, f"默认值应为 JSON 数组(array 类型),实际为 {_jtype(value)}")
        for i, item in enumerate(value):
            issue = validate_default(item, node.items, f"{path}[{i}]")
            if issue is not None:
                return issue
        return None

    if isinstance(node, Record):
        if not isinstance(value, dict):
            return Issue(
                path,
                f"默认值应为 JSON 对象(record {node.name}),实际为 {_jtype(value)}",
            )
        for f in node.fields:
            if f.name in value:
                issue = validate_default(
                    value[f.name], f.type, f"{path}.{f.name}"
                )
                if issue is not None:
                    return issue
            elif not f.has_default:
                return Issue(
                    f"{path}.{f.name}",
                    f"record {node.name} 的默认值缺少无默认值的必填字段 '{f.name}'",
                )
        return None

    return Issue(path, "内部错误:未知节点类型")


def _validate_primitive_default(value: Any, name: str, path: str) -> Optional[Issue]:
    if name == "null":
        if value is not None:
            return Issue(path, f"null 类型的默认值必须是 null,实际为 {_jtype(value)}")
        return None
    if name == "boolean":
        if not isinstance(value, bool):
            return Issue(path, f"boolean 类型的默认值必须是 true/false,实际为 {_jtype(value)}")
        return None
    if name == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            return Issue(path, f"int 类型的默认值必须是整数,实际为 {_jtype(value)}")
        if not (INT_MIN <= value <= INT_MAX):
            return Issue(path, f"默认值 {value} 超出 int32 范围")
        return None
    if name == "long":
        if isinstance(value, bool) or not isinstance(value, int):
            return Issue(path, f"long 类型的默认值必须是整数,实际为 {_jtype(value)}")
        if not (LONG_MIN <= value <= LONG_MAX):
            return Issue(path, f"默认值 {value} 超出 int64 范围")
        return None
    if name == "string":
        if not isinstance(value, str):
            return Issue(path, f"string 类型的默认值必须是字符串,实际为 {_jtype(value)}")
        return None
    return Issue(path, f"内部错误:未知基础类型 '{name}'")


def _jtype(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__
