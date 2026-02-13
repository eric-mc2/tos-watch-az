"""
End-to-end pipeline test from DIFF_CLEAN to JUDGE_CLEAN.

Parameterized tests covering all combinations of input data variations.
"""
import pytest
from typing import Optional, List
from dataclasses import dataclass

from src.services.blob import BlobService
from src.services.llm import LLMService, TOKEN_LIMIT
from src.services.embedding import EmbeddingService
from src.transforms.llm_transform import LLMTransform, create_llm_parser, create_llm_activity_processor
from src.transforms.differ import DiffDoc, DiffSection
from src.stages import Stage

from schemas.summary.v0 import MODULE as SUMMARY_MODULE
from schemas.fact.v0 import CLAIMS_MODULE, PROOF_MODULE
from schemas.judge.v0 import MODULE as JUDGE_MODULE

from schemas.summary.v2 import Substantive as SSubstantive
from schemas.summary.v4 import Summary
from schemas.fact.v1 import Claims, Fact, merge_facts
from schemas.judge.v1 import Judgement, Substantive as JSubstantive

from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.adapters.embedding.fake_client import FakeEmbeddingAdapter


class MockInputStream:
    """Mimics Azure Functions InputStream for blob triggers."""
    def __init__(self, blob_service, blob_name):
        self.name = blob_name
        self._service = blob_service
        
    def read(self):
        text = self._service.load_text_blob(self.name)
        return text.encode()


@dataclass
class PipelineTestCase:
    """Test case configuration for pipeline execution."""
    name: str
    diff_sections: List[DiffSection]
    summary_response: Optional[str]  # JSON or invalid string
    claims_response: Optional[str]   # JSON or invalid string
    fact_response: Optional[str]
    judge_response: Optional[str]
    expect_summary_success: str
    expect_claims_success: str
    expect_fact_success: str
    expect_judge_success: str
    num_claims: int


@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter()
    adapter.create_container()
    return BlobService(adapter)


@pytest.fixture
def fake_llm():
    adapter = FakeLLMAdapter()
    return LLMService(adapter)


@pytest.fixture
def fake_embedding():
    adapter = FakeEmbeddingAdapter()
    return EmbeddingService(adapter)


@pytest.fixture
def llm_transform(fake_storage, fake_llm):
    return LLMTransform(fake_storage, fake_llm)


def run_pipeline_stage_summarizer(fake_storage, llm_transform, diff_blob_path, company, policy, timestamp):
    """Run summarizer stage: DIFF_CLEAN -> SUMMARY_RAW."""
    from src.transforms.summary.summarizer import Summarizer
    from src.transforms.icl import ICL
    
    summarizer = Summarizer(fake_storage, ICL(fake_storage), llm_transform)
    
    processor = create_llm_activity_processor(
        fake_storage,
        summarizer.summarize,
        Stage.SUMMARY_RAW.value,
        "summarizer"
    )
    
    processor({'task_id': diff_blob_path, 'company': company, 'policy': policy, 'timestamp': timestamp})
    return f"{Stage.SUMMARY_RAW.value}/{company}/{policy}/{timestamp}/latest.txt"


def run_pipeline_stage_summary_parser(fake_storage, fake_llm, summary_raw_path):
    """Run summary parser: SUMMARY_RAW -> SUMMARY_CLEAN."""
    input_blob = MockInputStream(fake_storage, summary_raw_path)
    parser = create_llm_parser(fake_storage, fake_llm, SUMMARY_MODULE, Stage.SUMMARY_CLEAN.value)
    parser(input_blob)
    
    parts = fake_storage.parse_blob_path(summary_raw_path)
    return f"{Stage.SUMMARY_CLEAN.value}/{parts.company}/{parts.policy}/{parts.timestamp}/latest.json"


def run_pipeline_stage_claim_extractor(fake_storage, llm_transform, summary_clean_path, company, policy, timestamp):
    """Run claim extractor: SUMMARY_CLEAN -> CLAIM_RAW."""
    from src.transforms.factcheck.claim_extractor import ClaimExtractor
    
    claim_extractor = ClaimExtractor(storage=fake_storage, executor=llm_transform)
    
    processor = create_llm_activity_processor(
        fake_storage,
        claim_extractor.extract_claims,
        Stage.CLAIM_RAW.value,
        "claim_extractor"
    )
    
    processor({'task_id': summary_clean_path, 'company': company, 'policy': policy, 'timestamp': timestamp})
    return f"{Stage.CLAIM_RAW.value}/{company}/{policy}/{timestamp}/latest.txt"


def run_pipeline_stage_claims_parser(fake_storage, fake_llm, claim_raw_path):
    """Run claims parser: CLAIM_RAW -> CLAIM_CLEAN."""
    input_blob = MockInputStream(fake_storage, claim_raw_path)
    parser = create_llm_parser(fake_storage, fake_llm, CLAIMS_MODULE, Stage.CLAIM_CLEAN.value)
    parser(input_blob)
    
    parts = fake_storage.parse_blob_path(claim_raw_path)
    return f"{Stage.CLAIM_CLEAN.value}/{parts.company}/{parts.policy}/{parts.timestamp}/latest.json"


def run_pipeline_stage_claim_checker(fake_storage, llm_transform, fake_embedding, claim_clean_path, company, policy, timestamp):
    """Run claim checker: CLAIM_CLEAN + DIFF_CLEAN -> FACTCHECK_RAW."""
    from src.transforms.factcheck.claim_checker import ClaimChecker
    
    claim_checker = ClaimChecker(storage=fake_storage, executor=llm_transform, embedder=fake_embedding)
    
    processor = create_llm_activity_processor(
        fake_storage,
        claim_checker.check_claim,
        Stage.FACTCHECK_RAW.value,
        "claim_checker",
        paired_input_stage=Stage.DIFF_CLEAN.value
    )
    
    processor({'task_id': claim_clean_path, 'company': company, 'policy': policy, 'timestamp': timestamp})
    return f"{Stage.FACTCHECK_RAW.value}/{company}/{policy}/{timestamp}/latest.txt"


def run_pipeline_stage_fact_parser(fake_storage, fake_llm, fact_raw_path):
    """Run fact parser: FACTCHECK_RAW -> FACTCHECK_CLEAN."""
    input_blob = MockInputStream(fake_storage, fact_raw_path)
    parser = create_llm_parser(fake_storage, fake_llm, PROOF_MODULE, Stage.FACTCHECK_CLEAN.value, merge_facts)
    parser(input_blob)
    
    parts = fake_storage.parse_blob_path(fact_raw_path)
    return f"{Stage.FACTCHECK_CLEAN.value}/{parts.company}/{parts.policy}/{parts.timestamp}/latest.json"


def run_pipeline_stage_judge(fake_storage, llm_transform, fact_clean_path, company, policy, timestamp):
    """Run judge: FACTCHECK_CLEAN + SUMMARY_CLEAN -> JUDGE_RAW."""
    from src.transforms.factcheck.judge import Judge
    
    judge = Judge(storage=fake_storage, executor=llm_transform)
    
    processor = create_llm_activity_processor(
        fake_storage,
        judge.judge,
        Stage.JUDGE_RAW.value,
        "judge",
        paired_input_stage=Stage.SUMMARY_CLEAN.value
    )
    
    processor({'task_id': fact_clean_path, 'company': company, 'policy': policy, 'timestamp': timestamp})
    return f"{Stage.JUDGE_RAW.value}/{company}/{policy}/{timestamp}/latest.txt"


def run_pipeline_stage_judge_parser(fake_storage, fake_llm, judge_raw_path):
    """Run judge parser: JUDGE_RAW -> JUDGE_CLEAN."""
    input_blob = MockInputStream(fake_storage, judge_raw_path)
    parser = create_llm_parser(fake_storage, fake_llm, JUDGE_MODULE, Stage.JUDGE_CLEAN.value)
    parser(input_blob)
    
    parts = fake_storage.parse_blob_path(judge_raw_path)
    return f"{Stage.JUDGE_CLEAN.value}/{parts.company}/{parts.policy}/{parts.timestamp}/latest.json"


# Test data generation
def generate_test_cases():
    """Generate all combinations of test axes."""
    
    # Axis 1: DiffDoc variations
    one_diff = [DiffSection(index=0, before="Old text.", after="New text.")]
    two_short_diffs = [
        DiffSection(index=0, before="Users must be 13.", after="Users must be 18."),
        DiffSection(index=1, before="We share data.", after="We sell data.")
    ]
    two_long_diffs = [
        DiffSection(index=0, before="A" * (TOKEN_LIMIT//3), after="B" * (TOKEN_LIMIT//3)),
        DiffSection(index=1, before="C" * (TOKEN_LIMIT//3), after="D" * (TOKEN_LIMIT//3))
    ]
    empty_diff = []
    
    # Axis 2: Summary responses
    summary_true = Summary(practically_substantive=SSubstantive(rating=True, reason="Valid")).model_dump_json()
    summary_false = Summary(practically_substantive=SSubstantive(rating=False, reason="Not substantive")).model_dump_json()
    summary_invalid = "{invalid json"
    
    # Axis 3: Claims responses
    claims_zero = Claims(claims=[]).model_dump_json()
    claims_one = Claims(claims=["Claim 1"]).model_dump_json()
    claims_two = Claims(claims=["Claim 1", "Claim 2"]).model_dump_json()
    claims_invalid = "not json at all"
    
    # Axis 4: Fact responses
    fact_valid = Fact(claim="Test", veracity=True, reason="Valid").model_dump_json()
    fact_invalid = '{"ionses'
    
    # Axis 5: Judge responses
    judge_valid = Judgement(practically_substantive=JSubstantive(rating=True, reason="Ok")).model_dump_json()
    judge_invalid = "malformed"
    
    test_cases = [
        # No summarizer inputs
        # Empty Diff -- Everything after doesn't matter
        PipelineTestCase(
            name="empty_diff",
            diff_sections=empty_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='False',
            expect_claims_success='False',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=1
        ),
        # Single summarizer prompts
        # One Diff -- everything else happy
        PipelineTestCase(
            name="one_diff",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=1
        ),
        # Two Diffs -- everything else happy
        PipelineTestCase(
            name="two_diffs",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=1
        ),
        # One Diff + Summary False
        PipelineTestCase(
            name="one_diff_summary_false",
            diff_sections=one_diff,
            summary_response=summary_false,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='skip',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=1
        ),
        # One Diff + Summary Invalid
        PipelineTestCase(
            name="one_diff_summary_inv",
            diff_sections=one_diff,
            summary_response=summary_invalid,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='exit',
            expect_claims_success='False',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=1
        ),
        # Chunked summarizer prompts
        # Long Diffs + Summary True
        PipelineTestCase(
            name="long_diffs_summary_true",
            diff_sections=two_long_diffs,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=1
        ),
        # Long Diffs + Summary False
        PipelineTestCase(
            name="long_diffs_summary_false",
            diff_sections=two_long_diffs,
            summary_response=summary_false,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='skip',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=1
        ),
        # Long Diffs + Summary Invalid
        PipelineTestCase(
            name="long_diffs_summary_inv",
            diff_sections=two_long_diffs,
            summary_response=summary_invalid,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='exit',
            expect_claims_success='False',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=1
        ),
        # Claims Tests
        # Zero Claims
        PipelineTestCase(
            name="zero_claims",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_zero,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=0
        ),
        # One Claims
        PipelineTestCase(
            name="one_claims",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=1
        ),
        # Two Claims
        PipelineTestCase(
            name="two_claims", 
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_two,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=2
        ),
        # Invalid Claims
        PipelineTestCase(
            name="inv_claims",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_invalid,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='exit',
            expect_fact_success='False',
            expect_judge_success='False',
            num_claims=1
        ),
        # FactCheck Tests
        # Facts Valid
        PipelineTestCase(
            name="facts_valid",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=1
        ),
        # Facts Valid
        PipelineTestCase(
            name="facts_inv",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_invalid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='exit',
            expect_judge_success='False',
            num_claims=1
        ),
        # Judge Tests
        # Judge Valid
        PipelineTestCase(
            name="judge_valid",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_valid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='True',
            num_claims=1
        ),
        # Judge Invalid
        PipelineTestCase(
            name="judge_invalid",
            diff_sections=one_diff,
            summary_response=summary_true,
            claims_response=claims_one,
            fact_response=fact_valid,
            judge_response=judge_invalid,
            expect_summary_success='True',
            expect_claims_success='True',
            expect_fact_success='True',
            expect_judge_success='exit',
            num_claims=1
        ),
    ]
    
    return test_cases


@pytest.mark.parametrize("test_case", generate_test_cases(), ids=lambda tc: tc.name)
def test_pipeline_end_to_end_parameterized(fake_storage, fake_llm, llm_transform, fake_embedding, test_case):
    """
    Parameterized end-to-end pipeline test covering all input combinations.
    """
    company = "testco"
    policy = "terms"
    timestamp = "20260101000000"
    
    # Setup: Create DIFF_CLEAN blob
    diff_doc = DiffDoc(diffs=test_case.diff_sections)
    
    # Exit if empty
    if not diff_doc.diffs:
        return
    
    diff_blob_path = f"{Stage.DIFF_CLEAN.value}/{company}/{policy}/{timestamp}.json"
    fake_storage.upload_text_blob(diff_doc.model_dump_json(), diff_blob_path, metadata={})
    
    # Stage 1 & 2: Summarizer + Parser
    fake_llm.adapter.set_response(test_case.summary_response)
    
    summary_raw_path = run_pipeline_stage_summarizer(fake_storage, llm_transform, diff_blob_path, company, policy, timestamp)
    summary_clean_path = run_pipeline_stage_summary_parser(fake_storage, fake_llm, summary_raw_path)
    
    if test_case.expect_summary_success == 'True':
        assert fake_storage.check_blob(summary_clean_path)
    elif test_case.expect_summary_success == 'False':
        assert not fake_storage.check_blob(summary_clean_path)
        assert False, "Summary unexpectedly succeeded"
    elif test_case.expect_summary_success == 'exit':
        assert not fake_storage.check_blob(summary_clean_path)
        return
            
    # Stage 3 & 4: ClaimExtractor + Parser
    fake_llm.adapter.set_response(test_case.claims_response)
    
    claim_raw_path = run_pipeline_stage_claim_extractor(fake_storage, llm_transform, summary_clean_path, company, policy, timestamp)

    if test_case.expect_claims_success == 'skip':
        assert not fake_storage.check_blob(claim_raw_path)
        return

    claim_clean_path = run_pipeline_stage_claims_parser(fake_storage, fake_llm, claim_raw_path)
    
    if test_case.expect_claims_success == 'True':
        assert fake_storage.check_blob(claim_clean_path)
    elif test_case.expect_claims_success == 'False':
        assert not fake_storage.check_blob(claim_clean_path)
        assert False, "Claim Extractor unexpectedly succeeded"
    elif test_case.expect_claims_success == 'exit':
        assert not fake_storage.check_blob(claim_clean_path)
        return
        
    claims = Claims.model_validate_json(fake_storage.load_text_blob(claim_clean_path))
    assert len(claims.claims) == test_case.num_claims
    
    # Skip fact/judge if zero claims
    if test_case.num_claims == 0:
        return
    
    # Stage 5 & 6: ClaimChecker + Parser
    fake_llm.adapter.set_response(test_case.fact_response)
    
    fact_raw_path = run_pipeline_stage_claim_checker(fake_storage, llm_transform, fake_embedding, claim_clean_path, company, policy, timestamp)
    
    if test_case.expect_fact_success == 'skip':
        assert not fake_storage.check_blob(fact_raw_path)
        return
    
    fact_clean_path = run_pipeline_stage_fact_parser(fake_storage, fake_llm, fact_raw_path)
    
    if test_case.expect_fact_success == 'True':
        assert fake_storage.check_blob(fact_clean_path)
    elif test_case.expect_fact_success == 'False':
        assert not fake_storage.check_blob(fact_clean_path)
        assert False, "Claim checker unexpectedly succeeded"
    elif test_case.expect_fact_success == 'exit':
        assert not fake_storage.check_blob(fact_clean_path)
        return
            
    # Stage 7 & 8: Judge + Parser
    fake_llm.adapter.set_response(test_case.judge_response)
    
    judge_raw_path = run_pipeline_stage_judge(fake_storage, llm_transform, fact_clean_path, company, policy, timestamp)

    if test_case.expect_judge_success == 'skip':
        assert not fake_storage.check_blob(judge_raw_path)
        return
    
    judge_clean_path = run_pipeline_stage_judge_parser(fake_storage, fake_llm, judge_raw_path)
    
    if test_case.expect_judge_success == 'True':
        assert fake_storage.check_blob(judge_clean_path)
    elif test_case.expect_judge_success == 'False':
        assert not fake_storage.check_blob(judge_clean_path)
        assert False, "Judge unexpectedly succeeded"
    elif test_case.expect_judge_success == 'exit':
        assert not fake_storage.check_blob(judge_clean_path)
        return
    
    judgement = Judgement.model_validate_json(fake_storage.load_text_blob(judge_clean_path))
    assert judgement.practically_substantive is not None
    