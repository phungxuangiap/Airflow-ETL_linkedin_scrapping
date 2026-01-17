from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, List
import os
import google.generativeai as genai
import json
from openai import OpenAI
import os
from cerebras.cloud.sdk import Cerebras

@dataclass
class HTMLIdentifier:
    tag_name: str 
    parent_tag_name: Optional[str] = None
    class_name: Optional[str] = None
    parent_class_name: Optional[str] = None
    attribute_name: Optional[str] = None

    @classmethod
    async def create(cls, raw_html: str, prompt):


        client = Cerebras(
            api_key="csk-3mwefk3ycjhwy3wh2f8dmyjjvpcxkvk59j338f39934hx9p4"
        )
        completion = client.chat.completions.create(
            messages=[{"role":"user","content":prompt}],
            model="llama-3.3-70b",
            max_completion_tokens=1024,
            temperature=0.2,
            top_p=1,
            stream=False
        )
        try:
            # clean_text = response.output_text.strip().replace('```json', '').replace('```', '')
            clean_text = completion.choices[0].message.content.strip().replace('```json', '').replace('```', '')
            json_output = json.loads(clean_text)
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            return None
        return cls(
            tag_name=json_output.get("tag_name"),
            attribute_name=json_output.get("attribute_name"),
            class_name=json_output.get("class_name"),
            parent_tag_name=json_output.get("parent_tag_name"),
            parent_class_name=json_output.get("parent_class_name")
        )