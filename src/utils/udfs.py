from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_text_length(text: str) -> str:
    """
    Categorizes a text string into Short, Medium, or Long.
    If text is missing or invalid, returns Unknown.
    """
    if not text or not isinstance(text, str):
        return "Unknown"
        
    length = len(text.strip())
    if length < 50:
        return "Short"
    elif 50 <= length <= 120:
        return "Medium"
    else:
        return "Long"

# Register the Python function as a Spark UDF
text_length_udf = udf(categorize_text_length, StringType())
