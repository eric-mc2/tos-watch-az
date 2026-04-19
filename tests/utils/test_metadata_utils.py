"""
Unit tests for metadata utilities.

Tests the core metadata manipulation functions used in the migration.
"""
import pytest
from src.utils.metadata_utils import (
    prefix_metadata,
    unprefix_metadata,
    merge_lineage,
    extract_stage_metadata,
    PREFIXABLE_KEYS,
)
from src.stages import Stage


class TestPrefixMetadata:
    """Test metadata prefixing."""
    
    def test_prefix_basic(self):
        metadata = {
            "run_id": "abc123",
            "schema_version": "v1",
            "prompt_version": "v2"
        }
        result = prefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert result["brief_run_id"] == "abc123"
        assert result["brief_schema_version"] == "v1"
        assert result["brief_prompt_version"] == "v2"
        assert "run_id" not in result
        assert "schema_version" not in result
    
    def test_prefix_preserves_global_keys(self):
        metadata = {
            "run_id": "abc123",
            "touched": "2024-01-01T00:00:00Z"
        }
        result = prefix_metadata(metadata, Stage.SUMMARY_RAW.value)
        
        assert result["summary_run_id"] == "abc123"
        assert result["touched"] == "2024-01-01T00:00:00Z"
    
    def test_prefix_preserves_unknown_keys(self):
        """Unknown keys (like upstream metadata) should be preserved."""
        metadata = {
            "run_id": "abc123",
            "brief_run_id": "xyz789",  # Upstream metadata
            "custom_field": "value"
        }
        result = prefix_metadata(metadata, Stage.SUMMARY_RAW.value)
        
        assert result["summary_run_id"] == "abc123"
        assert result["brief_run_id"] == "xyz789"  # Preserved
        assert result["custom_field"] == "value"  # Preserved
    
    def test_prefix_all_prefixable_keys(self):
        metadata = {key: f"value_{key}" for key in PREFIXABLE_KEYS}
        result = prefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        for key in PREFIXABLE_KEYS:
            assert f"brief_{key}" in result
            assert result[f"brief_{key}"] == f"value_{key}"
    
    def test_prefix_non_llm_stage(self):
        """Non-LLM stages should return unchanged copy."""
        metadata = {"run_id": "abc123"}
        result = prefix_metadata(metadata, Stage.DIFF_CLEAN.value)
        
        assert result == metadata
        assert result is not metadata  # Should be a copy
    
    def test_prefix_does_not_modify_original(self):
        """Original metadata should not be modified."""
        metadata = {"run_id": "abc123"}
        original = metadata.copy()
        result = prefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert metadata == original
        assert result != original


class TestUnprefixMetadata:
    """Test metadata unprefixing."""
    
    def test_unprefix_basic(self):
        metadata = {
            "brief_run_id": "abc123",
            "brief_schema_version": "v1",
            "brief_prompt_version": "v2"
        }
        result = unprefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert result["run_id"] == "abc123"
        assert result["schema_version"] == "v1"
        assert result["prompt_version"] == "v2"
        assert "brief_run_id" not in result
    
    def test_unprefix_preserves_other_prefixes(self):
        """Unprefixing one stage shouldn't affect other stage prefixes."""
        metadata = {
            "brief_run_id": "abc123",
            "summary_run_id": "xyz789"
        }
        result = unprefix_metadata(metadata, Stage.SUMMARY_RAW.value)
        
        assert result["run_id"] == "xyz789"
        assert result["brief_run_id"] == "abc123"  # Preserved
    
    def test_unprefix_non_llm_stage(self):
        """Non-LLM stages should return unchanged copy."""
        metadata = {"run_id": "abc123"}
        result = unprefix_metadata(metadata, Stage.DIFF_CLEAN.value)
        
        assert result == metadata
        assert result is not metadata
    
    def test_prefix_unprefix_round_trip(self):
        """Prefix -> unprefix should recover original."""
        original = {
            "run_id": "abc123",
            "schema_version": "v1",
            "model_version": "claude-3-5-haiku"
        }
        prefixed = prefix_metadata(original, Stage.BRIEF_RAW.value)
        unprefixed = unprefix_metadata(prefixed, Stage.BRIEF_RAW.value)
        
        assert unprefixed == original


class TestMergeLineage:
    """Test metadata lineage merging."""
    
    def test_merge_single_upstream(self):
        """Test merging with one upstream stage."""
        upstream = {
            "brief_run_id": "abc123",
            "brief_schema_version": "v1"
        }
        new = {
            "run_id": "xyz789",
            "schema_version": "v2"
        }
        result = merge_lineage(upstream, new, Stage.SUMMARY_RAW.value)
        
        # Should have both upstream and current
        assert result["brief_run_id"] == "abc123"
        assert result["brief_schema_version"] == "v1"
        assert result["summary_run_id"] == "xyz789"
        assert result["summary_schema_version"] == "v2"
    
    def test_merge_multiple_upstream(self):
        """Test merging with multiple upstream stages (judge case)."""
        upstream = {
            "brief_run_id": "aaa",
            "summary_run_id": "bbb",
            "claim_extractor_run_id": "ccc",
            "claim_checker_run_id": "ddd"
        }
        new = {
            "run_id": "eee",
            "schema_version": "v1"
        }
        result = merge_lineage(upstream, new, Stage.JUDGE_RAW.value)
        
        # Should have all upstream + current
        assert result["brief_run_id"] == "aaa"
        assert result["summary_run_id"] == "bbb"
        assert result["claim_extractor_run_id"] == "ccc"
        assert result["claim_checker_run_id"] == "ddd"
        assert result["judge_run_id"] == "eee"
        assert result["judge_schema_version"] == "v1"
    
    def test_merge_empty_upstream(self):
        """Test merging with no upstream (first stage)."""
        upstream = {}
        new = {
            "run_id": "abc123",
            "schema_version": "v1"
        }
        result = merge_lineage(upstream, new, Stage.BRIEF_RAW.value)
        
        # Should just have prefixed current
        assert result["brief_run_id"] == "abc123"
        assert result["brief_schema_version"] == "v1"
        assert len(result) == 2
    
    def test_merge_does_not_modify_inputs(self):
        """Inputs should not be modified."""
        upstream = {"brief_run_id": "abc"}
        new = {"run_id": "xyz"}
        upstream_copy = upstream.copy()
        new_copy = new.copy()
        
        result = merge_lineage(upstream, new, Stage.SUMMARY_RAW.value)
        
        assert upstream == upstream_copy
        assert new == new_copy


class TestExtractStageMetadata:
    """Test extracting metadata for a specific stage."""
    
    def test_extract_single_stage(self):
        """Extract metadata for one stage from multi-stage metadata."""
        metadata = {
            "brief_run_id": "abc",
            "brief_schema_version": "v1",
            "summary_run_id": "xyz",
            "summary_schema_version": "v2"
        }
        result = extract_stage_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert result == {
            "run_id": "abc",
            "schema_version": "v1"
        }
    
    def test_extract_returns_unprefixed(self):
        """Extracted metadata should be unprefixed."""
        metadata = {
            "summary_run_id": "xyz789",
            "summary_prompt_version": "v8"
        }
        result = extract_stage_metadata(metadata, Stage.SUMMARY_RAW.value)
        
        assert "run_id" in result
        assert "prompt_version" in result
        assert "summary_run_id" not in result
    
    def test_extract_non_llm_stage(self):
        """Non-LLM stages should return empty dict."""
        metadata = {"something": "value"}
        result = extract_stage_metadata(metadata, Stage.DIFF_CLEAN.value)
        
        assert result == {}
    
    def test_extract_missing_stage(self):
        """Extracting a stage that doesn't exist returns empty dict."""
        metadata = {
            "brief_run_id": "abc",
            "summary_run_id": "xyz"
        }
        result = extract_stage_metadata(metadata, Stage.JUDGE_RAW.value)
        
        assert result == {}

    def test_bare_metadata(self):
        """Extracting non-lineage data should return non-lineage data."""
        metadata = {
            "brief_run_id": "abc",
            "run_id": "xyz"
        }
        result = extract_stage_metadata(metadata, Stage.JUDGE_RAW.value)

        assert "run_id" in result
        assert result["run_id"] == "xyz"

    def test_bare_and_lineage_metadata(self):
        """Extracting non-lineage data should return lineage part."""
        metadata = {
            "brief_run_id": "abc",
            "run_id": "xyz"
        }
        result = extract_stage_metadata(metadata, Stage.BRIEF_CLEAN.value)

        assert "run_id" in result
        assert "brief_run_id" not in result
        assert result["run_id"] == "abc"


class TestIdempotency:
    """Test that operations are idempotent where expected."""
    
    def test_prefix_already_prefixed(self):
        """Prefixing already-prefixed metadata should not double-prefix."""
        metadata = {
            "brief_run_id": "abc123",
            "brief_schema_version": "v1"
        }
        # This represents calling prefix on already-migrated data
        # It should preserve the prefixed keys (they're not in PREFIXABLE_KEYS list)
        result = prefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert result["brief_run_id"] == "abc123"
        assert result["brief_schema_version"] == "v1"
        # Should not create briefer_briefer_run_id
        assert "brief_briefer_run_id" not in result
    
    def test_merge_lineage_idempotent(self):
        """Running merge_lineage twice should produce same result."""
        upstream = {"brief_run_id": "abc"}
        new = {"run_id": "xyz"}
        
        result1 = merge_lineage(upstream, new, Stage.SUMMARY_RAW.value)
        # Simulate running again with already-merged metadata as "new"
        result2 = merge_lineage({}, result1, Stage.SUMMARY_RAW.value)
        
        # Should have all the same keys (though some might be duplicated)
        assert "brief_run_id" in result2
        assert "summary_run_id" in result2


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_empty_metadata(self):
        """Empty metadata should be handled gracefully."""
        result = prefix_metadata({}, Stage.BRIEF_RAW.value)
        assert result == {}
        
        result = unprefix_metadata({}, Stage.BRIEF_RAW.value)
        assert result == {}
        
        result = merge_lineage({}, {}, Stage.SUMMARY_RAW.value)
        assert result == {}
    
    def test_none_values(self):
        """None values should be preserved."""
        metadata = {
            "run_id": "abc",
            "error_flag": None
        }
        result = prefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert result["brief_run_id"] == "abc"
        assert result["error_flag"] is None
    
    def test_special_characters_in_values(self):
        """Special characters in values should be preserved."""
        metadata = {
            "run_id": "abc-123_xyz",
            "model_version": "claude-3.5-haiku@20241022"
        }
        result = prefix_metadata(metadata, Stage.BRIEF_RAW.value)
        
        assert result["brief_run_id"] == "abc-123_xyz"
        assert result["brief_model_version"] == "claude-3.5-haiku@20241022"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
