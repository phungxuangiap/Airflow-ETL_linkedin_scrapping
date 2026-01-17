# Time to pause between scroll actions in seconds
SCROLL_PAUSE_TIME = 0.5 

# Distance to scroll in pixels
SCROLL_DISTANCE = 300    

# Gemini prompt for structured data extraction
GEMINI_STRUCTURE_EXTRACTION_PROMPT = """
    You are an expert at extracting structured data from unstructured text. Given the following text, extract the relevant information and present it in a structured format such as JSON or a table.
    Text:
    {input_text}
    Please provide the extracted data in a clear and organized manner.
"""


GEMINI_JOB_HEADER_EXTRACTION_PROMPT = """
    Role: HTML Structure & Data Parser.

    Task: Analyze the provided HTML snippet of a job listing card and identify the most stable, generalized structural patterns (tags, attributes, and classes) for specific data fields. This analysis must enable broad matching across all similar job cards on the entire website.

    Heuristics & Priorities (Ranked by Importance):
    1. Class Selection Strategy (CRITICAL): 
        * PRIORITIZE Semantic/Functional Classes: Look for classes that describe the content (e.g., "job-card__title", "company-name", "posting-date").
        * IGNORE Hash/Instance Classes: Strictly avoid classes that look like random strings or hashes (e.g., "css-123xyz", "_a6bc7d8") and unique IDs (e.g., "job-999").
        * PREFER Shared Utility Classes: If a descriptive class is missing, look for a utility class that is applied to all instances of this specific field (e.g., "text-bold", "link-overlay").
    2. Target Identification: For text fields, identify the innermost tag containing the text. For URLs or images, identify the attribute (href, src, data-src).
    3. Attribute Name: Use "text" if the value is the inner content of the tag. Use the specific attribute name (e.g., "href", "src", "title") if the value is stored within an attribute.
    4. Parent Context: Identify the immediate parent tag and its most stable class to provide a reliable hierarchical anchor for the selector.
    5. Missing Fields: If a field is not present in the provided HTML, return null for all its identifiers.

    Output Format (Return ONLY JSON):
    {
        "title": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "company": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "company_avatar_url": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "company_location": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "location_working_type": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "working_type": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "date_posted": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "number_applicants": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "job_url": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" },
        "description": { "tag_name": "", "attribute_name": "", "class_name": "", "parent_tag_name": "", "parent_class_name": "" }
    }

    Input HTML: {raw_html}
"""

GEMINI_JOB_DESCRIPTION_EXTRACTION_PROMPT = """
    
"""

GEMINI_JOB_URL_EXTRACTION_PROMPT = '''
    Role: HTML Structure & Data Parser.
    Task: Analyze the provided HTML snippet of a job listing card and identify the most stable, generalized structural patterns (tags, attributes, and classes) specifically to extract the Job ID or the Job Slug.

    Heuristics & Priorities:

    1. Identifier Selection Strategy (CRITICAL):

    * First Priority (Slug): If no ID attribute exists, identify the href attribute containing the descriptive URL path (slug).
    * Second Priority (Job ID): Search for functional attributes that store a numeric or unique ID (e.g., data-job-id, data-id, job-id, value).

    2. Class Selection Strategy:

    * STRICTLY IGNORE unique hashes, instance-specific IDs, or random strings (e.g., css-123xyz, _a6bc7d8, ember123).

    3. Target Identification: Identify the innermost tag and the specific attribute name containing the ID or Slug.
    4. Parent Context: Identify the immediate parent tag and its most stable class. If the target tag is the root element or has no identifiable parent in the provided snippet, set parent_tag_name and parent_class_name to null.

    Output Format (Return ONLY JSON):
    {
    "tag_name": "string",
    "attribute_name": "string",
    "class_name": "string or null",
    "parent_tag_name": "string or null",
    "parent_class_name": "string or null"
    }

    Input HTML:
'''
