"""核心兼容逻辑的单元测试(仅依赖标准库,可直接用 python3 运行)。"""

import unittest

from app.avro_schema import SchemaError, parse
from app.compat import check_compatibility
from app.demo import DEMO_RECURSIVE_REQUEST


def check(writer, reader):
    return check_compatibility(parse(writer), parse(reader))


class TestPrimitives(unittest.TestCase):
    def test_same_primitive_ok(self):
        self.assertIsNone(check("string", "string"))
        self.assertIsNone(check("int", "int"))
        self.assertIsNone(check("null", "null"))

    def test_int_promotes_to_long(self):
        self.assertIsNone(check("int", "long"))

    def test_long_not_demotable_to_int(self):
        issue = check("long", "int")
        self.assertIsNotNone(issue)
        self.assertIn("long", issue.reason)

    def test_other_primitive_mismatch(self):
        self.assertIsNotNone(check("string", "int"))
        self.assertIsNotNone(check("boolean", "string"))
        self.assertIsNotNone(check("int", "string"))


class TestRecords(unittest.TestCase):
    W = {
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "string"},
        ],
    }

    def test_identical_ok(self):
        self.assertIsNone(check(self.W, self.W))

    def test_name_mismatch(self):
        r = dict(self.W, name="S")
        issue = check(self.W, r)
        self.assertIsNotNone(issue)
        self.assertIn("名称不匹配", issue.reason)

    def test_writer_field_dropped_ok(self):
        r = {
            "type": "record",
            "name": "R",
            "fields": [{"name": "a", "type": "long"}],
        }
        self.assertIsNone(check(self.W, r))  # b 被忽略,a 提升为 long

    def test_added_field_with_default_ok(self):
        r = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "string", "default": "x"},
            ],
        }
        self.assertIsNone(check(self.W, r))

    def test_added_field_without_default_fails(self):
        r = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "string"},
            ],
        }
        issue = check(self.W, r)
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$.fields.c")
        self.assertIn("没有默认值", issue.reason)

    def test_added_field_bad_default_fails(self):
        r = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "int", "default": "not-an-int"},
            ],
        }
        issue = check(self.W, r)
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$.fields.c.default")

    def test_union_default_checked_against_first_branch(self):
        # 首分支是 string,默认值 1 不符 -> 不兼容
        r = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": ["string", "int"], "default": 1},
            ],
        }
        issue = check(self.W, r)
        self.assertIsNotNone(issue)
        self.assertIn("$.fields.c.default", issue.path)
        # 首分支是 string,默认值 "s" 相符 -> 兼容(即使第二分支是 int)
        r2 = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string"},
                {"name": "c", "type": ["string", "int"], "default": "s"},
            ],
        }
        self.assertIsNone(check(self.W, r2))

    def test_nested_field_type_mismatch_path(self):
        w = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "sub", "type": {
                    "type": "record", "name": "Sub",
                    "fields": [{"name": "x", "type": "long"}],
                }},
            ],
        }
        r = {
            "type": "record",
            "name": "R",
            "fields": [
                {"name": "sub", "type": {
                    "type": "record", "name": "Sub",
                    "fields": [{"name": "x", "type": "int"}],
                }},
            ],
        }
        issue = check(w, r)
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$.fields.sub.fields.x")


class TestUnions(unittest.TestCase):
    def test_writer_each_branch_must_be_readable(self):
        # writer 分支 string 在 reader union 中无可读分支
        issue = check(["int", "string"], ["int", "long"])
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$[1]")

    def test_reader_any_branch_match_ok(self):
        self.assertIsNone(check(["int"], ["int", "string"]))
        self.assertIsNone(check("int", ["string", "long"]))  # int->long

    def test_writer_union_vs_reader_single(self):
        self.assertIsNone(check(["int"], "long"))
        self.assertIsNone(check(["int", "null"], ["long", "null"]))
        self.assertIsNotNone(check(["int", "string"], "long"))

    def test_writer_single_vs_reader_union_fail(self):
        issue = check("boolean", ["int", "string"])
        self.assertIsNotNone(issue)


class TestArrays(unittest.TestCase):
    def test_array_items_compatible(self):
        w = {"type": "array", "items": "int"}
        r = {"type": "array", "items": "long"}
        self.assertIsNone(check(w, r))

    def test_array_items_incompatible(self):
        w = {"type": "array", "items": "string"}
        r = {"type": "array", "items": "long"}
        issue = check(w, r)
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$.items")

    def test_array_vs_non_array(self):
        self.assertIsNotNone(check({"type": "array", "items": "int"}, "int"))


class TestRecursion(unittest.TestCase):
    LINKED = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "v", "type": "int"},
            {"name": "next", "type": ["null", "Node"], "default": None},
        ],
    }

    def test_recursive_identical_ok(self):
        self.assertIsNone(check(self.LINKED, self.LINKED))

    def test_recursive_evolution_ok(self):
        r = {
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "v", "type": "long"},
                {"name": "next", "type": ["null", "Node"], "default": None},
                {"name": "tag", "type": "string", "default": ""},
            ],
        }
        self.assertIsNone(check(self.LINKED, r))

    def test_recursive_deep_incompatible_found(self):
        # 不兼容藏在递归引用的字段里:不能仅凭名称相同判定兼容
        r = {
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "v", "type": "string"},  # int -> string 不允许
                {"name": "next", "type": ["null", "Node"], "default": None},
            ],
        }
        issue = check(self.LINKED, r)
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$.fields.v")

    def test_recursive_branch_removed_fails(self):
        # reader 的 next union 去掉了 Node 分支:writer 的 Node 分支不可读
        r = {
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "v", "type": "int"},
                {"name": "next", "type": ["null", "string"], "default": None},
            ],
        }
        issue = check(self.LINKED, r)
        self.assertIsNotNone(issue)
        self.assertIn("$.fields.next", issue.path)

    def test_mutual_recursion(self):
        w = {
            "type": "record",
            "name": "A",
            "fields": [
                {"name": "b", "type": {
                    "type": "record",
                    "name": "B",
                    "fields": [{"name": "a", "type": ["null", "A"], "default": None}],
                }},
            ],
        }
        r_ok = {
            "type": "record",
            "name": "A",
            "fields": [
                {"name": "b", "type": {
                    "type": "record",
                    "name": "B",
                    "fields": [
                        {"name": "a", "type": ["null", "A"], "default": None},
                        {"name": "x", "type": "int", "default": 7},
                    ],
                }},
            ],
        }
        self.assertIsNone(check(w, r_ok))
        r_bad = {
            "type": "record",
            "name": "A",
            "fields": [
                {"name": "b", "type": {
                    "type": "record",
                    "name": "B",
                    "fields": [{"name": "a", "type": ["null", "int"], "default": None}],
                }},
            ],
        }
        self.assertIsNotNone(check(w, r_bad))

    def test_same_name_different_shape_not_compatible(self):
        # 名称相同但字段类型不同,必须报不兼容(反例:仅凭名称判定会误判)
        w = {
            "type": "record",
            "name": "Wrap",
            "fields": [
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [{"name": "x", "type": "int"}],
                }},
                {"name": "self", "type": ["null", "Wrap"], "default": None},
            ],
        }
        r = {
            "type": "record",
            "name": "Wrap",
            "fields": [
                {"name": "inner", "type": {
                    "type": "record", "name": "Inner",
                    "fields": [{"name": "x", "type": "string"}],
                }},
                {"name": "self", "type": ["null", "Wrap"], "default": None},
            ],
        }
        issue = check(w, r)
        self.assertIsNotNone(issue)
        self.assertEqual(issue.path, "$.fields.inner.fields.x")


class TestDefaults(unittest.TestCase):
    def _added(self, ftype, default):
        w = {"type": "record", "name": "R", "fields": []}
        r = {
            "type": "record",
            "name": "R",
            "fields": [{"name": "f", "type": ftype, "default": default}],
        }
        return check(w, r)

    def test_int_range(self):
        self.assertIsNone(self._added("int", 2**31 - 1))
        self.assertIsNotNone(self._added("int", 2**31))
        self.assertIsNone(self._added("long", 2**40))
        self.assertIsNotNone(self._added("long", 2**63))

    def test_bool_not_int(self):
        self.assertIsNotNone(self._added("int", True))

    def test_null_default(self):
        self.assertIsNone(self._added("null", None))
        self.assertIsNotNone(self._added("null", 0))

    def test_array_default(self):
        self.assertIsNone(self._added({"type": "array", "items": "int"}, [1, 2]))
        self.assertIsNotNone(self._added({"type": "array", "items": "int"}, [1, "x"]))

    def test_record_default(self):
        rt = {
            "type": "record",
            "name": "Sub",
            "fields": [
                {"name": "a", "type": "int"},
                {"name": "b", "type": "string", "default": "d"},
            ],
        }
        self.assertIsNone(self._added(rt, {"a": 1}))
        self.assertIsNotNone(self._added(rt, {"b": "x"}))  # 缺必填 a
        self.assertIsNotNone(self._added(rt, {"a": "x"}))


class TestSchemaErrors(unittest.TestCase):
    def test_unknown_type_name(self):
        with self.assertRaises(SchemaError) as cm:
            parse("NoSuch")
        self.assertEqual(cm.exception.path, "$")

    def test_namespace_rejected(self):
        with self.assertRaises(SchemaError) as cm:
            parse({"type": "record", "name": "R", "namespace": "x.y", "fields": []})
        self.assertEqual(cm.exception.path, "$.namespace")

    def test_aliases_rejected(self):
        with self.assertRaises(SchemaError) as cm:
            parse({"type": "record", "name": "R", "aliases": ["Q"], "fields": []})
        self.assertEqual(cm.exception.path, "$.aliases")

    def test_field_missing_type_located(self):
        with self.assertRaises(SchemaError) as cm:
            parse({
                "type": "record", "name": "R",
                "fields": [{"name": "a"}],
            })
        self.assertEqual(cm.exception.path, "$.fields[0]")

    def test_duplicate_record_name(self):
        with self.assertRaises(SchemaError):
            parse({
                "type": "record", "name": "R",
                "fields": [
                    {"name": "a", "type": {
                        "type": "record", "name": "R", "fields": []}},
                ],
            })

    def test_union_empty_and_nested(self):
        with self.assertRaises(SchemaError):
            parse([])
        with self.assertRaises(SchemaError):
            parse([["int"], "string"])

    def test_bad_name(self):
        with self.assertRaises(SchemaError) as cm:
            parse({"type": "record", "name": "1bad", "fields": []})
        self.assertEqual(cm.exception.path, "$.name")

    def test_forward_reference_rejected(self):
        with self.assertRaises(SchemaError):
            parse({
                "type": "record", "name": "A",
                "fields": [{"name": "x", "type": "NotYetDefined"}],
            })


class TestDemo(unittest.TestCase):
    def test_recursive_demo_is_compatible(self):
        self.assertIsNone(
            check(DEMO_RECURSIVE_REQUEST["writer"], DEMO_RECURSIVE_REQUEST["reader"])
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
