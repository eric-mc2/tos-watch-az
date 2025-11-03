import re
import json
from typing import Optional, Any

def extract_json_from_response(response: str) -> dict:
    """
    Extract JSON with additional context about the extraction process.
    
    Args:
        response: The chatbot response string
        
    Returns:
        Dictionary containing:
        - 'success': boolean indicating if JSON was successfully extracted
        - 'data': the parsed JSON object (if successful)
        - 'raw_match': the raw string that was matched (if any)
        - 'error': error message (if unsuccessful)
    """
    result = {
        'success': False,
        'data': None,
        'raw_match': None,
        'error': None
    }
    
    if not response or not isinstance(response, str):
        result['error'] = 'Invalid input: response must be a non-empty string'
        return result
    
    # Try object pattern first
    json_pattern = r'\{[^{}]*?(?:\{[^{}]*?\}[^{}]*?)*\}'
    matches = re.findall(json_pattern, response, re.DOTALL)
    
    for match in matches:
        result['raw_match'] = match
        try:
            cleaned_match = re.sub(r'\s+', ' ', match).strip()
            result['data'] = json.loads(cleaned_match)
            result['success'] = True
            return result
        except json.JSONDecodeError as e:
            # Store the first error encountered
            if not result['error']:
                result['error'] = f'JSON decode error: {e}'
    
    # Try array pattern
    array_pattern = r'\[[^\[\]]*(?:\[[^\[\]]*\][^\[\]]*)*\]'
    array_matches = re.findall(array_pattern, response, re.DOTALL)
    
    for match in array_matches:
        result['raw_match'] = match
        try:
            cleaned_match = re.sub(r'\s+', ' ', match).strip()
            result['data'] = json.loads(cleaned_match)
            result['success'] = True
            return result
        except json.JSONDecodeError as e:
            if not result['error']:
                result['error'] = f'JSON decode error: {e}'
    
    if not result['error']:
        result['error'] = 'No valid JSON structure found in response'
    
    return result

