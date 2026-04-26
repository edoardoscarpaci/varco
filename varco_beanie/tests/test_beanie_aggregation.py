"""
Unit tests for varco_beanie.query.aggregation
===============================================
Covers ``BeanieAggregationApplicator``.

No MongoDB connection is required — the applicator is fully synchronous and
operates on plain Python ``list[dict]`` pipeline objects.

Sections
--------
- Construction                   — defaults, allowed_fields, compiler injection
- ``apply_query``                — $match stage; None node; AST compilation
- ``apply_sort``                 — $sort stage; None / empty; multi-field
- ``apply_pagination``           — $skip + $limit; None values; zero offset
- Pipeline composition           — apply_query → apply_sort → apply_pagination
- allowed_fields enforcement     — ValueError on unlisted field
"""

from __future__ import annotations

import pytest

from varco_core.query.builder import QueryBuilder
from varco_core.query.type import SortField, SortOrder
from varco_beanie.query.aggregation import BeanieAggregationApplicator, Pipeline


# ── Construction ──────────────────────────────────────────────────────────────


class TestBeanieAggregationApplicatorConstruction:
    def test_default_construction(self) -> None:
        app = BeanieAggregationApplicator()
        assert app.allowed_fields == set()

    def test_with_allowed_fields(self) -> None:
        app = BeanieAggregationApplicator(allowed_fields={"status", "created_at"})
        assert "status" in app.allowed_fields
        assert "created_at" in app.allowed_fields

    def test_repr_contains_class_name(self) -> None:
        app = BeanieAggregationApplicator()
        assert "BeanieAggregationApplicator" in repr(app)


# ── apply_query ───────────────────────────────────────────────────────────────


class TestBeanieAggregationApplicatorApplyQuery:
    def test_none_node_returns_pipeline_unchanged(self) -> None:
        app = BeanieAggregationApplicator()
        result = app.apply_query([], None)
        assert result == []

    def test_none_node_preserves_existing_stages(self) -> None:
        app = BeanieAggregationApplicator()
        existing = [{"$project": {"name": 1}}]
        result = app.apply_query(existing, None)
        assert result == existing

    def test_equality_node_adds_match_stage(self) -> None:
        app = BeanieAggregationApplicator()
        node = QueryBuilder().eq("status", "active").build()
        result = app.apply_query([], node)
        assert len(result) == 1
        assert "$match" in result[0]
        assert result[0]["$match"] == {"status": "active"}

    def test_gt_node_adds_match_stage(self) -> None:
        app = BeanieAggregationApplicator()
        node = QueryBuilder().gt("age", 18).build()
        result = app.apply_query([], node)
        assert result[0]["$match"] == {"age": {"$gt": 18}}

    def test_and_node_compiles_correctly(self) -> None:
        app = BeanieAggregationApplicator()
        node = QueryBuilder().eq("status", "active").gt("age", 18).build()
        result = app.apply_query([], node)
        match = result[0]["$match"]
        assert "$and" in match

    def test_apply_query_does_not_mutate_input_pipeline(self) -> None:
        """apply_query() returns a new list — the input is not modified."""
        app = BeanieAggregationApplicator()
        original: Pipeline = []
        node = QueryBuilder().eq("x", 1).build()
        result = app.apply_query(original, node)
        assert original == []
        assert len(result) == 1

    def test_match_placed_at_end_of_existing_pipeline(self) -> None:
        """$match is appended — caller controls position by choosing when to call."""
        app = BeanieAggregationApplicator()
        existing = [{"$project": {"name": 1}}]
        node = QueryBuilder().eq("status", "active").build()
        result = app.apply_query(existing, node)
        assert len(result) == 2
        assert "$project" in result[0]
        assert "$match" in result[1]


# ── apply_sort ────────────────────────────────────────────────────────────────


class TestBeanieAggregationApplicatorApplySort:
    def test_none_sort_returns_pipeline_unchanged(self) -> None:
        app = BeanieAggregationApplicator()
        result = app.apply_sort([], None)
        assert result == []

    def test_empty_sort_list_returns_pipeline_unchanged(self) -> None:
        app = BeanieAggregationApplicator()
        result = app.apply_sort([], [])
        assert result == []

    def test_single_asc_sort(self) -> None:
        app = BeanieAggregationApplicator()
        sort_fields = [SortField("created_at", SortOrder.ASC)]
        result = app.apply_sort([], sort_fields)
        assert len(result) == 1
        assert result[0] == {"$sort": {"created_at": 1}}

    def test_single_desc_sort(self) -> None:
        app = BeanieAggregationApplicator()
        sort_fields = [SortField("created_at", SortOrder.DESC)]
        result = app.apply_sort([], sort_fields)
        assert result[0] == {"$sort": {"created_at": -1}}

    def test_multi_field_sort_combined_in_one_stage(self) -> None:
        """Multiple sort fields are combined in a single $sort stage."""
        app = BeanieAggregationApplicator()
        sort_fields = [
            SortField("category", SortOrder.ASC),
            SortField("created_at", SortOrder.DESC),
        ]
        result = app.apply_sort([], sort_fields)
        assert len(result) == 1
        sort_doc = result[0]["$sort"]
        assert sort_doc["category"] == 1
        assert sort_doc["created_at"] == -1

    def test_apply_sort_does_not_mutate_input(self) -> None:
        app = BeanieAggregationApplicator()
        original: Pipeline = []
        app.apply_sort(original, [SortField("x", SortOrder.ASC)])
        assert original == []


# ── apply_pagination ──────────────────────────────────────────────────────────


class TestBeanieAggregationApplicatorApplyPagination:
    def test_none_limit_and_none_offset_no_stages_added(self) -> None:
        app = BeanieAggregationApplicator()
        result = app.apply_pagination([], None, None)
        assert result == []

    def test_limit_only(self) -> None:
        app = BeanieAggregationApplicator()
        result = app.apply_pagination([], 10, None)
        assert result == [{"$limit": 10}]

    def test_offset_only(self) -> None:
        app = BeanieAggregationApplicator()
        result = app.apply_pagination([], None, 20)
        assert result == [{"$skip": 20}]

    def test_offset_before_limit(self) -> None:
        """$skip must appear before $limit in the pipeline."""
        app = BeanieAggregationApplicator()
        result = app.apply_pagination([], 10, 20)
        assert len(result) == 2
        assert "$skip" in result[0]
        assert "$limit" in result[1]
        assert result[0]["$skip"] == 20
        assert result[1]["$limit"] == 10

    def test_zero_offset_not_added(self) -> None:
        """offset=0 is falsy — no $skip stage should be added."""
        app = BeanieAggregationApplicator()
        result = app.apply_pagination([], 5, 0)
        assert len(result) == 1
        assert "$limit" in result[0]

    def test_apply_pagination_does_not_mutate_input(self) -> None:
        app = BeanieAggregationApplicator()
        original: Pipeline = []
        app.apply_pagination(original, 5, 10)
        assert original == []


# ── allowed_fields enforcement ────────────────────────────────────────────────


class TestBeanieAggregationApplicatorAllowedFields:
    def test_unlisted_field_raises_value_error(self) -> None:
        app = BeanieAggregationApplicator(allowed_fields={"status"})
        node = QueryBuilder().eq("secret_field", "value").build()
        with pytest.raises(ValueError, match="secret_field"):
            app.apply_query([], node)

    def test_listed_field_passes(self) -> None:
        app = BeanieAggregationApplicator(allowed_fields={"status"})
        node = QueryBuilder().eq("status", "active").build()
        result = app.apply_query([], node)
        assert len(result) == 1


# ── Pipeline composition ──────────────────────────────────────────────────────


class TestBeanieAggregationApplicatorComposition:
    def test_full_pipeline_composition(self) -> None:
        """apply_query → apply_sort → apply_pagination builds a complete pipeline."""
        app = BeanieAggregationApplicator()

        node = QueryBuilder().eq("status", "active").build()
        sort_fields = [SortField("created_at", SortOrder.DESC)]

        pipeline: Pipeline = []
        pipeline = app.apply_query(pipeline, node)
        pipeline = app.apply_sort(pipeline, sort_fields)
        pipeline = app.apply_pagination(pipeline, limit=10, offset=20)

        assert len(pipeline) == 4
        assert "$match" in pipeline[0]
        assert "$sort" in pipeline[1]
        assert "$skip" in pipeline[2]
        assert "$limit" in pipeline[3]

    def test_caller_can_prepend_custom_stages(self) -> None:
        """Callers can add custom stages before calling the applicator."""
        app = BeanieAggregationApplicator()

        # Start with a $lookup stage
        pipeline: Pipeline = [{"$lookup": {"from": "users", "as": "owner"}}]
        node = QueryBuilder().eq("status", "active").build()
        pipeline = app.apply_query(pipeline, node)

        assert len(pipeline) == 2
        assert "$lookup" in pipeline[0]
        assert "$match" in pipeline[1]

    def test_caller_can_append_custom_stages(self) -> None:
        """Callers can add custom stages after the applicator."""
        app = BeanieAggregationApplicator()

        node = QueryBuilder().eq("status", "active").build()
        pipeline = app.apply_query([], node)
        pipeline = pipeline + [{"$group": {"_id": "$category", "count": {"$sum": 1}}}]

        assert len(pipeline) == 2
        assert "$match" in pipeline[0]
        assert "$group" in pipeline[1]
