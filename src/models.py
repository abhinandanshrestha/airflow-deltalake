from pydantic import BaseModel


# Response model
class RAGResponse(BaseModel):
    query: str
    response: str
    sources: set
    retriever: list[dict]