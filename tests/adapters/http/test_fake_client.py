import pytest
from src.adapters.http.fake_client import FakeHttpAdapter


class TestFakeHttpAdapter:
    """Test suite for FakeHttpAdapter"""

    def test_default_response_returns_200(self):
        """Test that default response returns 200 status code"""
        adapter = FakeHttpAdapter()
        response = adapter.get("https://example.com")
        
        assert response.status_code == 200
        assert response.content == b''

    def test_configure_default_response(self):
        """Test configuring a default response"""
        adapter = FakeHttpAdapter()
        adapter.configure_default_response(
            status_code=404,
            text="Not Found"
        )
        
        response = adapter.get("https://example.com")
        assert response.status_code == 404
        assert response.text == "Not Found"

    def test_configure_specific_url_response(self):
        """Test configuring response for a specific URL"""
        adapter = FakeHttpAdapter()
        adapter.configure_response(
            url="https://example.com/api",
            status_code=201,
            text="Created"
        )
        
        response = adapter.get("https://example.com/api")
        assert response.status_code == 201
        assert response.text == "Created"
        
        # Different URL should return default
        other_response = adapter.get("https://example.com/other")
        assert other_response.status_code == 200

    def test_configure_json_response(self):
        """Test configuring JSON response"""
        adapter = FakeHttpAdapter()
        test_data = {"key": "value", "number": 42}
        adapter.configure_response(
            url="https://api.example.com",
            json_data=test_data
        )
        
        response = adapter.get("https://api.example.com")
        assert response.status_code == 200
        assert response.json() == test_data
        assert response.headers['Content-Type'] == 'application/json'

    def test_configure_error_response(self):
        """Test configuring error responses"""
        adapter = FakeHttpAdapter()
        adapter.configure_error(status_code=500)
        
        response = adapter.get("https://example.com")
        assert response.status_code == 500

    def test_error_until_call_limit(self):
        """Test error response with call count limit"""
        adapter = FakeHttpAdapter()
        adapter.configure_default_response(status_code=200, text="Success")
        adapter.configure_error(status_code=503, until_call=2)
        
        # First two calls should return error
        response1 = adapter.get("https://example.com")
        assert response1.status_code == 503
        
        response2 = adapter.get("https://example.com")
        assert response2.status_code == 503
        
        # Third call should return default response
        response3 = adapter.get("https://example.com")
        assert response3.status_code == 200
        assert response3.text == "Success"

    def test_reset_clears_all_configuration(self):
        """Test that reset clears all configuration"""
        adapter = FakeHttpAdapter()
        
        # Configure various responses
        adapter.configure_response(
            url="https://example.com",
            status_code=201,
            text="Created"
        )
        adapter.configure_default_response(status_code=404)
        adapter.configure_error(status_code=500, until_call=5)
        
        # Reset
        adapter.reset()
        
        # Should return default 200 response
        response = adapter.get("https://example.com")
        assert response.status_code == 200
        assert response.content == b''

    def test_custom_headers(self):
        """Test configuring custom headers"""
        adapter = FakeHttpAdapter()
        custom_headers = {"X-Custom-Header": "value"}
        adapter.configure_default_response(
            status_code=200,
            text="OK",
            headers=custom_headers
        )
        
        response = adapter.get("https://example.com")
        assert response.headers['X-Custom-Header'] == "value"

    def test_bytes_response(self):
        """Test response with bytes content"""
        adapter = FakeHttpAdapter()
        byte_content = b'\x00\x01\x02\x03'
        adapter.configure_response(
            url="https://example.com/binary",
            text=byte_content
        )
        
        response = adapter.get("https://example.com/binary")
        assert response.content == byte_content
