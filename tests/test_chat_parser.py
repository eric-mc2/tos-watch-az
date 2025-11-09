from src.chat_parser import extract_json_from_response
    
def test_case_1():
    # Case 1: JSON with prefix and suffix
    input = "Sure, here's the data you requested: {\"name\": \"John\", \"age\": 30, \"city\": \"New York\"}. Let me know if you need anything else!"
    result = extract_json_from_response(input)
    assert result['success'], result['error']
        
def test_case_2():
    # Case 2: Multiple JSON objects
    input = "First: {\"a\": 1}, Second: {\"b\": 2}"
    result = extract_json_from_response(input)
    assert result['success'], result['error']
        
def test_case_3():
    # Case 3: Nested JSON
    input = "The result is: {\"user\": {\"name\": \"Alice\", \"preferences\": {\"theme\": \"dark\"}}}"
    result = extract_json_from_response(input)
    assert result['success'], result['error']
        
def test_case_4():
    # Case 4: JSON array
    input = "Here are the items: [\"apple\", \"banana\", \"cherry\"]"
    result = extract_json_from_response(input)
    assert result['success'], result['error']
        
def test_case_5():
    # Case 5: No JSON
    input = "I'm sorry, I couldn't find any data matching your request."
    result = extract_json_from_response(input)
    assert not result['success'], result['error']
        
def test_case_6():
    # Case 6: Malformed JSON
    input = "Data: {\"name\": \"John\", \"age\": 30,}"
    result = extract_json_from_response(input)
    assert not result['success'], result['error']
        
def test_case_7():
    # Case 7: Truncated JSON
    input = "Partial result: {\"name\": \"John\", \"age\":"
    result = extract_json_from_response(input)
    assert not result['success'], result['error']
        
def test_case_8():
    # Case 8: JSON with special characters
    input = "Result: {\"message\": \"Hello \\\"world\\\"!\", \"value\": 42.5}"
    result = extract_json_from_response(input)
    assert result['success'], result['error']
        
def test_case_9():
    # Case 9: Empty response
    input = ""
    result = extract_json_from_response(input)
    assert not result['success'], result['error']
        
        
def test_case_10():
    # Case 10: Complex nested structure
    input = "Analysis: {\"users\": [{\"id\": 1, \"data\": {\"scores\": [85, 92, 78]}}]}"
    result = extract_json_from_response(input)
    assert result['success'], result['error']
    
