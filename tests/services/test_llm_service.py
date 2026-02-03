import pytest
from src.services.llm import LLMService
from src.clients.llm.fake_client import FakeLLMAdapter

@pytest.fixture
def fake_llm():
    return FakeLLMAdapter()

@pytest.fixture
def llm_service(fake_llm):
    return LLMService(fake_llm)

class TestJsonParser:

    def test_case_1(self, llm_service):
        # Case 1: JSON with prefix and suffix
        input = "Sure, here's the data you requested: {\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}. Let me know if you need anything else!"
        result = llm_service.extract_json_from_response(input)
        assert result['success'], result['error']

    def test_case_2(self, llm_service):
        # Case 2: Multiple JSON objects
        input = "First: {\"a\": 1}, Second: {\"b\": 2}"
        result = llm_service.extract_json_from_response(input)
        assert result['success'], result['error']

    def test_case_3(self, llm_service):
        # Case 3: Nested JSON
        input = "The result is: {\"user\": {\"name\": \"Alice\", \"preferences\": {\"theme\": \"dark\"}}}"
        result = llm_service.extract_json_from_response(input)
        assert result['success'], result['error']

    def test_case_4(self, llm_service):
        # Case 4: JSON array
        input = "Here are the items: [\"apple\", \"banana\", \"cherry\"]"
        result = llm_service.extract_json_from_response(input)
        assert result['success'], result['error']

    def test_case_5(self, llm_service):
        # Case 5: No JSON
        input = "I'm sorry, I couldn't find any data matching your request."
        result = llm_service.extract_json_from_response(input)
        assert not result['success'], result['error']

    def test_case_6(self, llm_service):
        # Case 6: Malformed JSON
        input = "Data: {\"name\": \"John\", \"age\": 30,}"
        result = llm_service.extract_json_from_response(input)
        assert not result['success'], result['error']

    def test_case_7(self, llm_service):
        # Case 7: Truncated JSON
        input = "Partial result: {\"name\": \"John\", \"age\":"
        result = llm_service.extract_json_from_response(input)
        assert not result['success'], result['error']

    def test_case_8(self, llm_service):
        # Case 8: JSON with special characters
        input = "Result: {\"message\": \"Hello \\\"world\\\"!\", \"value\": 42.5}"
        result = llm_service.extract_json_from_response(input)
        assert result['success'], result['error']

    def test_case_9(self, llm_service):
        # Case 9: Empty response
        input = ""
        result = llm_service.extract_json_from_response(input)
        assert not result['success'], result['error']

    def test_case_10(self, llm_service):
        # Case 10: Complex nested structure
        input = "Analysis: {\"users\": [{\"id\": 1, \"data\": {\"scores\": [85, 92, 78]}}]}"
        result = llm_service.extract_json_from_response(input)
        assert result['success'], result['error']

class TestSanitizer:

    def test_sanitizer(self, llm_service):
        assert "bad" == llm_service.sanitize_response("bad")
        assert True == llm_service.sanitize_response(True)
        assert 123 == llm_service.sanitize_response(123)
        assert ["good", "stuff"] == llm_service.sanitize_response(["good", "stuff"])
        assert {"good": "stuff"} == llm_service.sanitize_response({"good":"stuff"})
        assert {"good": {"stuff": "here"}} == llm_service.sanitize_response({"good":{"stuff":"here"}})
        assert {"good": ["stuff", "here"]} == llm_service.sanitize_response({"good":["stuff","here"]})