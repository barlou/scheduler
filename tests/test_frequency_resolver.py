# tests/test_frequency_resolver.py
"""Tests for frequency_resolver.py."""
from __future__ import annotations

import pytest

from framework.frequency_resolver import (
    FrequencyRelation,
    ProcessingStrategy,
    cron_interval_seconds,
    compare_frequencies,
    resolve_upstream_frequency,
    split_into_chunks,
)


# ─────────────────────────────────────────────────────────────────────────────
# cron_interval_seconds
# ─────────────────────────────────────────────────────────────────────────────

class TestCronIntervalSeconds:
    def test_hourly(self):
        assert cron_interval_seconds("0 * * * *") == pytest.approx(3600, abs=1)

    def test_daily(self):
        assert cron_interval_seconds("0 0 * * *") == pytest.approx(86400, abs=1)

    def test_weekly(self):
        assert cron_interval_seconds("0 0 * * 0") == pytest.approx(604800, abs=60)

    def test_invalid_expression_raises(self):
        with pytest.raises(ValueError):
            cron_interval_seconds("not a cron")


# ─────────────────────────────────────────────────────────────────────────────
# compare_frequencies
# ─────────────────────────────────────────────────────────────────────────────

class TestCompareFrequencies:
    def test_upstream_less_frequent_is_mandatory(self):
        # weekly upstream, daily downstream
        relation, up_s, down_s = compare_frequencies("0 0 * * 0", "0 1 * * *")
        assert relation == FrequencyRelation.MANDATORY
        assert up_s > down_s

    def test_upstream_more_frequent_is_accumulation(self):
        # hourly upstream, daily downstream
        relation, up_s, down_s = compare_frequencies("0 * * * *", "0 0 * * *")
        assert relation == FrequencyRelation.ACCUMULATION
        assert up_s < down_s

    def test_equal_frequencies(self):
        relation, up_s, down_s = compare_frequencies("0 1 * * *", "0 2 * * *")
        assert relation == FrequencyRelation.EQUAL
        assert up_s == pytest.approx(down_s, abs=1)


# ─────────────────────────────────────────────────────────────────────────────
# split_into_chunks
# ─────────────────────────────────────────────────────────────────────────────

class TestSplitIntoChunks:
    def test_even_split(self):
        files  = ["a", "b", "c", "d"]
        chunks = split_into_chunks(files, chunk_size=2)
        assert chunks == [["a", "b"], ["c", "d"]]

    def test_uneven_split(self):
        files  = ["a", "b", "c", "d", "e"]
        chunks = split_into_chunks(files, chunk_size=2)
        assert chunks == [["a", "b"], ["c", "d"], ["e"]]

    def test_chunk_size_larger_than_list(self):
        files  = ["a", "b"]
        chunks = split_into_chunks(files, chunk_size=10)
        assert chunks == [["a", "b"]]

    def test_empty_list(self):
        assert split_into_chunks([], chunk_size=5) == []

    def test_invalid_chunk_size_raises(self):
        with pytest.raises(ValueError):
            split_into_chunks(["a"], chunk_size=0)


# ─────────────────────────────────────────────────────────────────────────────
# resolve_upstream_frequency
# ─────────────────────────────────────────────────────────────────────────────

class TestResolveUpstreamFrequency:
    def test_mandatory_no_files_blocks(self):
        # weekly upstream, daily downstream, no pending files
        result = resolve_upstream_frequency(
            upstream_job_id="ingestor",
            upstream_cron=   "0 0 * * 0",
            downstream_cron= "0 1 * * *",
            pending_files=   [],
            chunk_threshold= 10,
        )
        assert result.relation   == FrequencyRelation.MANDATORY
        assert result.strategy   == ProcessingStrategy.BLOCK
        assert result.is_blocked is True

    def test_mandatory_with_file_sequential(self):
        result = resolve_upstream_frequency(
            upstream_job_id="ingestor",
            upstream_cron=   "0 0 * * 0",
            downstream_cron= "0 1 * * *",
            pending_files=   ["data_2026-05-03.parquet"],
            chunk_threshold= 10,
        )
        assert result.relation   == FrequencyRelation.MANDATORY
        assert result.strategy   == ProcessingStrategy.SEQUENTIAL
        assert result.is_blocked is False

    def test_accumulation_below_threshold_sequential(self):
        files  = [f"file_{i}_2026-05-0{i+1}.parquet" for i in range(5)]
        result = resolve_upstream_frequency(
            upstream_job_id="ingestor",
            upstream_cron=   "0 * * * *",   # hourly
            downstream_cron= "0 0 * * *",   # daily
            pending_files=   files,
            chunk_threshold= 10,
        )
        assert result.relation == FrequencyRelation.ACCUMULATION
        assert result.strategy == ProcessingStrategy.SEQUENTIAL

    def test_accumulation_above_threshold_parallel(self):
        files  = [f"file_{i}_2026-05-03.parquet" for i in range(15)]
        result = resolve_upstream_frequency(
            upstream_job_id="ingestor",
            upstream_cron=   "0 * * * *",
            downstream_cron= "0 0 * * *",
            pending_files=   files,
            chunk_threshold= 10,
        )
        assert result.relation == FrequencyRelation.ACCUMULATION
        assert result.strategy == ProcessingStrategy.PARALLEL

    def test_equal_frequency_sequential(self):
        result = resolve_upstream_frequency(
            upstream_job_id="ingestor",
            upstream_cron=   "0 1 * * *",
            downstream_cron= "0 2 * * *",
            pending_files=   ["file_2026-05-03.parquet"],
            chunk_threshold= 10,
        )
        assert result.relation == FrequencyRelation.EQUAL
        assert result.strategy == ProcessingStrategy.SEQUENTIAL

    def test_pending_files_preserved_in_result(self):
        files  = ["a_2026-05-01.parquet", "b_2026-05-02.parquet"]
        result = resolve_upstream_frequency(
            upstream_job_id="ingestor",
            upstream_cron=   "0 * * * *",
            downstream_cron= "0 0 * * *",
            pending_files=   files,
            chunk_threshold= 10,
        )
        assert result.pending_files == files
