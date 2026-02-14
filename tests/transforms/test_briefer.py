from typing import List

import pytest
import json
import re

from schemas.brief.v1 import Memo, Brief
from schemas.llmerror.v1 import LLMError
from schemas.summary.v3 import Summary as SummaryV3, VERSION as VERSIONV3
from schemas.summary.v4 import Summary as SummaryV4, VERSION
from schemas.summary.v2 import Summary as SummaryV2, Substantive
from schemas.fact.v1 import Claims
from src.adapters.llm.protocol import Message
from src.stages import Stage
from src.transforms.differ import DiffSection, DiffDoc
from src.transforms.factcheck.claim_extractor import ClaimExtractorBuilder, ClaimExtractor
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService, TOKEN_LIMIT
from src.transforms.llm_transform import LLMTransform
from src.transforms.summary.briefer import BriefBuilder, Briefer


@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter()
    adapter.create_container()
    service = BlobService(adapter)
    return service


@pytest.fixture
def fake_llm():
    return FakeLLMAdapter()


@pytest.fixture
def llm_service(fake_llm):
    return LLMService(fake_llm)


@pytest.fixture
def llm_transform(fake_storage, llm_service):
    return LLMTransform(fake_storage, llm_service)

class TestBriefBuilder:
    # Verify that some chunking happens, but leave in-depth verificaiton to DiffChunker tests.

    def test_single_short(self, fake_storage, llm_service, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, llm_service)
        data = DiffDoc(diffs=[DiffSection(index=0, before="before", after="after")])
        
        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) == 1

    def test_multiple_short(self, fake_storage, llm_service, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, llm_service)
        data = DiffDoc(diffs=[DiffSection(index=0, before="before", after="after")]*10)

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) == 1

    def test_single_long(self, fake_storage, llm_service, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, llm_service)
        data = DiffDoc(diffs=[DiffSection(index=0,
                                          before="before"*int(TOKEN_LIMIT*.9),
                                          after="after")])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) == 1

    def test_multiple_long(self, fake_storage, llm_service, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, llm_service)
        data = DiffDoc(diffs=[DiffSection(index=0,
                                          before="before" * int(TOKEN_LIMIT * .9),
                                          after="after")]*3)

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) == 3

class TestBriefer:

    def test_single(self, fake_storage, llm_service, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=0,
                                           before="before",
                                           after="after")])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Configure fake llm response
        response = Memo(relevance_flag=False,
                        section_memo="stuff",
                        running_memo="stuff")
        llm_service.adapter.set_response_static(response.model_dump_json())

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        result = Brief.model_validate_json(result_json)
        assert len(result.memos) == 1
        assert len(fake_storage.adapter.list_blobs()) == 1

    def test_multiple_short(self, fake_storage, llm_service, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=0,
                                           before="before",
                                           after="after")]*20)

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Configure fake llm response
        response = Memo(relevance_flag=False,
                        section_memo="stuff",
                        running_memo="stuff")
        llm_service.adapter.set_response_static(response.model_dump_json())

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        result = Brief.model_validate_json(result_json)
        assert len(result.memos) == 1  # they are short!
        assert len(fake_storage.adapter.list_blobs()) == 1

    def test_multiple_long(self, fake_storage, llm_service, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=i,
                                           before="before",
                                           after="after")
                               for i in range(TOKEN_LIMIT)])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        out_blob_name = f"{Stage.BRIEF_RAW.value}/company/policy/12345/latest.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name, metadata={"schema_version": VERSION})

        # Configure fake llm response
        def respond(system: str, messages: List[Message]):
            return Memo(relevance_flag=False,
                        section_memo=messages[1].content,
                        running_memo=messages[0].content)
        llm_service.adapter.set_response_func(respond)

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        result = Brief.model_validate_json(result_json)
        assert len(result.memos) > 1

        # Should overwrite with last memo
        assert len(fake_storage.adapter.list_blobs()) == 1
        raw_txt = fake_storage.load_text_blob(out_blob_name)
        raw_data = Memo.model_validate_json(raw_txt)
        assert f"Section: {TOKEN_LIMIT - 1}" in raw_data.section_memo

        # Should carry over last memo
        raw_txt = fake_storage.load_text_blob(out_blob_name)
        raw_data = Memo.model_validate_json(raw_txt)
        index = re.search(r"Section: (\d+)", raw_data.running_memo).group(1)
        assert index and int(index) > 0 and int(index) < TOKEN_LIMIT - 1




