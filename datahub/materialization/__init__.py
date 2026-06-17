"""Materialization layer: transform immutable raw archives into query layers.

The raw zip archive under ``local_data/.../raw`` is the immutable source layer.
Materialization reads it and produces a Parquet query/materialized layer that
DuckDB reads directly. CSV exists only transiently inside the zip parsing flow;
no persistent CSV is ever written to the materialized tree.
"""
