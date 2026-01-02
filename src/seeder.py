import logging
from src.blob_utils import upload_json_blob
from src.log_utils import setup_logger
from src.scraper_utils import validate_url, sanitize_urlpath
import json

logger = setup_logger(__name__, logging.INFO)

STATIC_URLS = {
"google":[
    "https://safety.google/intl/en/content-safety/",
    "https://safety.google/intl/en/security/built-in-protection/",
    "https://safety.google/intl/en/privacy/privacy-controls/",
    "https://safety.google/intl/en/security-privacy/",
    "https://safety.google/intl/en/cybersecurity-advancements/",
    "https://safety.google/intl/en/families/",
    "https://safety.google/intl/en_us/safety/",
    "https://safety.google/intl/en/principles/",
    "https://policies.google.com/faq?hl=en",
    "https://policies.google.com/?hl=en",
    "https://policies.google.com/terms?hl=en",
    "https://policies.google.com/privacy?hl=en"
],
"meta": [
    "https://transparency.meta.com/policies/other-policies/transfer-your-information",
    "https://transparency.meta.com/policies/community-standards/additional-protection-minors/",
    "https://transparency.meta.com/policies/community-standards/cybersecurity/",
    "https://transparency.meta.com/policies/community-standards/violent-graphic-content/",
    "https://transparency.meta.com/policies/other-policies/meta-AI-disclosures",
    "https://transparency.meta.com/policies/other-policies",
    "https://transparency.meta.com/policies/community-standards/user-requests/",
    "https://transparency.meta.com/policies/community-standards/locally-illegal-products-services",
    "https://transparency.meta.com/policies/community-standards/meta-intellectual-property",
    "https://transparency.meta.com/policies/community-standards/intellectual-property/",
    "https://transparency.meta.com/policies/community-standards/spam/",
    "https://transparency.meta.com/policies/community-standards/misinformation/",
    "https://transparency.meta.com/policies/community-standards/memorialization/",
    "https://transparency.meta.com/policies/community-standards/inauthentic-behavior/",
    "https://transparency.meta.com/policies/community-standards/authentic-identity-representation",
    "https://transparency.meta.com/policies/community-standards/account-integrity",
    "https://transparency.meta.com/policies/community-standards",
    "https://www.meta.com/people-practices/meta-political-engagement/",
    "https://www.facebook.com/legal/terms/",
    "https://mbasic.facebook.com/privacy/policy/printable/"
],
"openai": [
    "https://openai.com/safety/",
    "https://openai.com/policies/",
    "https://openai.com/safety/how-we-think-about-safety-alignment/",
    "https://openai.com/security-and-privacy/",
    "https://openai.com/policies/how-chatgpt-and-our-foundation-models-are-developed/",
    "https://openai.com/policies/how-your-data-is-used-to-improve-model-performance/",
    "https://openai.com/enterprise-privacy/",
    "https://openai.com/policies/using-operator-in-line-with-our-policies/",
    "https://openai.com/policies/creating-images-and-videos-in-line-with-our-policies/",
    "https://openai.com/policies/usage-policies/",
    "https://openai.com/policies/services-agreement/",
    "https://openai.com/policies/data-processing-addendum/",
    "https://openai.com/policies/service-terms/",
    "https://openai.com/policies/privacy-policy/",
    "https://openai.com/policies/terms-of-use/"
],
"canva": [
    "https://www.canva.com/policies/data-processing-addendum/",
    "https://www.canva.com/policies/ai-product-terms/",
    "https://www.canva.com/policies/terms-of-use/",
    "https://www.canva.com/policies/contributor-agreement/",
    "https://www.canva.com/policies/affinity-additional-terms/",
    "https://www.canva.com/policies/content-license-agreement/",
    "https://www.canva.com/policies/acceptable-use-policy/",
    "https://www.canva.com/policies/privacy-policy/",
    "https://www.canva.com/trust/education/",
    "https://www.canva.com/trust/compliance/",
    "https://www.canva.com/trust/legal/",
    "https://www.canva.com/trust/safety/",
    "https://www.canva.com/trust/privacy/",
    "https://www.canva.com/security/",
    "https://www.canva.com/trust/"
],
"grammarly": [
    "https://www.grammarly.com/technical-specifications",
    "https://www.grammarly.com/ai/responsible-ai",
    "https://www.grammarly.com/compliance",
    "https://www.grammarly.com/privacy-policy",
    "https://www.grammarly.com/security",
    "https://www.grammarly.com/privacy",
    "https://www.grammarly.com/trust"
],
"anthropic": [
    "https://www.anthropic.com/legal/consumer-terms"
]
}


def validate_urls(urls: dict):
    for company, urls in urls.items():
        for url in urls:
            if not validate_url(url):
                raise ValueError(f"Invalid url: {url}")
            fp = sanitize_urlpath(url)
            if not fp:
                raise ValueError(f"Invalid url -> filename: {url} -> {fp}")

def sanitize_urls(urls: dict):
    return {url: f"{company}/{sanitize_urlpath(url)}" for company, urls in urls.items() for url in urls}
    
def seed_urls(urls: dict = STATIC_URLS):
    validate_urls(urls)
    upload_json_blob(json.dumps(urls, indent=2), 'static_urls.json')
    url_paths = sanitize_urls(urls)
    upload_json_blob(json.dumps(url_paths, indent=2), 'url_blob_paths.json')