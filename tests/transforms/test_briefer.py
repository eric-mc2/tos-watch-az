from typing import List

import pytest
import json
import re

from schemas.brief.v0 import BRIEF_MODULE
from schemas.brief.v1 import Memo, Brief, merge_memos
from schemas.llmerror.v1 import LLMError
from src.adapters.llm.protocol import Message
from src.stages import Stage
from src.transforms.differ import DiffSection, DiffDoc
from src.adapters.storage.fake_client import FakeStorageAdapter
from src.adapters.llm.fake_client import FakeLLMAdapter
from src.services.blob import BlobService
from src.services.llm import LLMService, TOKEN_LIMIT
from src.transforms.llm_transform import LLMTransform, create_llm_parser
from src.transforms.summary.briefer import BriefBuilder, Briefer


@pytest.fixture
def fake_storage():
    adapter = FakeStorageAdapter()
    adapter.create_container()
    service = BlobService(adapter)
    return service


@pytest.fixture
def fake_llm():
    return LLMService(FakeLLMAdapter())


@pytest.fixture
def llm_transform(fake_storage, fake_llm):
    return LLMTransform(fake_storage, fake_llm)


class TestBriefBuilder:
    # Verify that some chunking happens, but leave in-depth verificaiton to DiffChunker tests.

    def test_single_short(self, fake_storage, fake_llm):
        # Arrange
        builder = BriefBuilder(fake_storage, fake_llm)
        data = DiffDoc(diffs=[DiffSection(index=0, before="before", after="after")])
        
        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) == 1

    def test_multiple_short(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, fake_llm)
        data = DiffDoc(diffs=[DiffSection(index=0, before="before", after="after")]*10)

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) == 1

    def test_single_long(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, fake_llm)
        data = DiffDoc(diffs=[DiffSection(index=0,
                                          before="before"*int(TOKEN_LIMIT*.9),
                                          after="after")])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) >= 2  # oversized section is split into multiple chunks

    def test_multiple_long(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        builder = BriefBuilder(fake_storage, fake_llm)
        data = DiffDoc(diffs=[DiffSection(index=0,
                                          before="before" * int(TOKEN_LIMIT * .9),
                                          after="after")]*3)

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Act
        prompts = list(builder.build_prompt(blob_name))
        assert len(prompts) >= 3  # each oversized section is split into multiple chunks

class TestBriefer:

    def test_single(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=0,
                                           before="before",
                                           after="after")])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Configure fake llm response
        response = Memo(relevance_flag=False,
                        section_memo="stuff",
                        running_memo="stuff")
        fake_llm.adapter.set_response_static(response.model_dump_json())

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # Validate as memo because there is only one.
        Memo.model_validate_json(result_json)

    def test_multiple_short(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=0,
                                           before="before",
                                           after="after")]*20)

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Configure fake llm response
        response = Memo(relevance_flag=False,
                        section_memo="stuff",
                        running_memo="stuff")
        fake_llm.adapter.set_response_static(response.model_dump_json())

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        # validate as memo because not chunked
        Memo.model_validate_json(result_json)

    def test_multiple_long(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=i,
                                           before="before",
                                           after="after")
                               for i in range(TOKEN_LIMIT)])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Configure fake llm response
        response = Memo(relevance_flag=False,
                        section_memo="stuff",
                        running_memo="stuff")
        fake_llm.adapter.set_response_static(response.model_dump_json())

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)
        parser = create_llm_parser(fake_llm, BRIEF_MODULE, merge_memos)
        result_json, metadata = parser(result_json, metadata)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        result = Brief.model_validate_json(result_json)
        assert len(result.memos) > 1

    def test_invalid_json(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=i,
                                           before="before",
                                           after="after")
                               for i in range(TOKEN_LIMIT)])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Configure fake llm response
        fake_llm.adapter.set_response_static('{"field":{"incomplete}')

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)
        assert metadata['error_flag'] is not None
        LLMError.model_validate_json(result_json)
    
    def test_long_response(self, fake_storage, fake_llm, llm_transform):
        # Arrange
        data  = DiffDoc(diffs=[DiffSection(index=i,
                                           before="before",
                                           after="after")
                               for i in range(TOKEN_LIMIT)])

        blob_name = f"{Stage.DIFF_CLEAN.value}/company/policy/12345.json"
        data_serialized = data.model_dump_json()
        fake_storage.upload_text_blob(data_serialized, blob_name)

        # Configure fake llm response
        overhead = Memo(relevance_flag=False,
                        section_memo="",
                        running_memo="").model_dump_json()
        repeats = fake_llm.adapter.get_max_output() - len(overhead)
        fake_resp = Memo(relevance_flag=False,
                         section_memo="1"*repeats,
                         running_memo="")
        fake_llm.adapter.set_response_static(fake_resp.model_dump_json())

        briefer = Briefer(fake_storage, llm_transform)

        # Act
        result_json, metadata = briefer.brief(blob_name)
        parser = create_llm_parser(fake_llm, BRIEF_MODULE, merge_memos)
        result_json, metadata = parser(result_json, metadata)

        # Assert
        assert isinstance(metadata, dict)
        assert "schema_version" in metadata
        assert "prompt_version" in metadata

        result = Brief.model_validate_json(result_json)
        assert len(result.memos) > 1