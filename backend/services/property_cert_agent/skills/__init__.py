from .attachment_page_skill import extract as extract_attachment_page
from .cover_page_skill import extract as extract_cover_page
from .mortgage_page_skill import extract as extract_mortgage_page
from .new_real_estate_cert_skill import extract as extract_new_real_estate_cert
from .old_shanghai_property_cert_skill import extract as extract_old_shanghai_property_cert

__all__ = [
    "extract_attachment_page",
    "extract_cover_page",
    "extract_mortgage_page",
    "extract_new_real_estate_cert",
    "extract_old_shanghai_property_cert",
]
