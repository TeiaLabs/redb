import pandas as pd


def validate_dataframe(df: pd.DataFrame) -> list[str]:
    """
    Returns: missing fields if any
    """
    required_fields = {
        'content', 'key', 'url', 'kb_name',
        'content_embedding.vector', 'content_embedding.model_name', 
        'content_embedding.model_type',
    }
    optional_fields = {
        'query_embedding.vector',
        'query_embedding.model_name',
        'query_embedding.model_type',
        'data_type',
    }
    missing_required = list(required_fields - set(df.columns))
    missing_optional = list(optional_fields - set(df.columns))

    return missing_required, missing_optional
