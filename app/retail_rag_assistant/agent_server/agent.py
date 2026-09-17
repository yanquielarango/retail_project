import logging
import os
from typing import AsyncGenerator

import mlflow

from agents import (
    Agent,
    Runner,
    function_tool,
    set_default_openai_api,
    set_default_openai_client,
)
from agents.tracing import set_trace_processors
from databricks.sdk import WorkspaceClient
from databricks_openai import AsyncDatabricksOpenAI
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from agent_server.history import normalize_history_items
from agent_server.utils import (
    get_session_id,
    process_agent_stream_events,
)


logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Databricks App resources
# -------------------------------------------------------------------

RAG_INDEX_NAME = os.environ["RAG_INDEX_NAME"]
LLM_ENDPOINT = os.environ["LLM_ENDPOINT"]


RETRIEVAL_COLUMNS = [
    "chunk_id",
    "chunk_to_retrieve",
    "document_type",
    "source_uri",
    "product_id",
    "category",
]


# -------------------------------------------------------------------
# OpenAI Agents SDK configuration
# -------------------------------------------------------------------

set_default_openai_client(
    AsyncDatabricksOpenAI()
)

set_default_openai_api(
    "chat_completions"
)

set_trace_processors([])

mlflow.openai.autolog()

logging.getLogger(
    "mlflow.utils.autologging_utils"
).setLevel(logging.ERROR)


# -------------------------------------------------------------------
# RAG retrieval tool
# -------------------------------------------------------------------

@function_tool
def search_retail_knowledge(
    question: str,
) -> str:
    """
    Search the Retail Data Platform knowledge base.

    Use this tool to retrieve information about:

    - products and product information
    - product sales performance
    - suppliers and categories
    - Retail Data Platform architecture
    - Bronze, Silver, and Gold layers
    - Lakeflow pipelines
    - data quality and DQX
    - testing and validation
    - platform documentation
    """

    if not question.strip():
        return "No question was provided."

    workspace = WorkspaceClient()

    response = workspace.vector_search_indexes.query_index(
        index_name=RAG_INDEX_NAME,
        query_text=question,
        columns=RETRIEVAL_COLUMNS,
        num_results=10,
        query_type="HYBRID",
    )

    response_dict = response.as_dict()

    manifest = response_dict.get(
        "manifest",
        {},
    )

    result = response_dict.get(
        "result",
        {},
    )

    manifest_columns = manifest.get(
        "columns",
        [],
    )

    rows = result.get(
        "data_array",
        [],
    )

    if not rows:
        return (
            "The available knowledge does not contain "
            "enough information."
        )

    column_names = [
        column["name"]
        for column in manifest_columns
    ]

    documents = [
        dict(zip(column_names, row))
        for row in rows
    ]

    context_parts = []

    for document in documents:
        source_uri = (
            document.get("source_uri")
            or "Unknown source"
        )

        document_type = (
            document.get("document_type")
            or "Unknown"
        )

        product_id = (
            document.get("product_id")
            or "N/A"
        )

        category = (
            document.get("category")
            or "N/A"
        )

        content = (
            document.get("chunk_to_retrieve")
            or ""
        )

        context_parts.append(
            f"Source: {source_uri}\n"
            f"Document type: {document_type}\n"
            f"Product ID: {product_id}\n"
            f"Category: {category}\n"
            f"Content:\n{content}"
        )

    return "\n\n---\n\n".join(
        context_parts
    )


# -------------------------------------------------------------------
# Agent instructions
# -------------------------------------------------------------------

AGENT_INSTRUCTIONS = (
    "You are the AI assistant for the Retail Data Platform. "

    "For every factual question about the Retail Data Platform, "
    "products, sales, suppliers, architecture, pipelines, "
    "Bronze, Silver, Gold, data quality, DQX, testing, "
    "or documentation, you MUST call the "
    "search_retail_knowledge tool before answering. "

    "This also applies to factual follow-up questions. "

    "Your answer MUST be grounded exclusively in the content "
    "returned by search_retail_knowledge. "

    "Never add facts based on general knowledge, assumptions, "
    "geography, company names, or your own reasoning. "

    "Never add a currency symbol or currency code unless the "
    "retrieved context explicitly states the currency. "

    "If the retrieved context contains a numeric value without "
    "a currency, reproduce the value without adding PLN, EUR, "
    "USD, or any other currency. "

    "When the user asks about one specific product, answer only "
    "about that product unless the user explicitly asks for "
    "a comparison. "

    "When the user asks what products are available, "
    "list the specific products found in the retrieved context "
    "instead of describing only the product table structure. "

    "If product names are present in the retrieved context, "
    "include them in the answer. "

    "Never rank products or use claims such as highest, lowest, "
    "best, worst, most, or least unless the user explicitly asks "
    "for a comparison or ranking and the retrieved context "
    "directly supports it. "

    "Do not calculate or derive new metrics unless the user "
    "explicitly asks for a calculation. "

    "IMPORTANT: Invalid Silver records may be quarantined. "

    "Never say that all Lakeflow Expectations must pass before "
    "Gold can run. "

    "Individual records may fail validation and be moved to "
    "quarantine without causing the entire pipeline to fail. "

    "Clearly distinguish row-level validation failures from "
    "pipeline-level quality gate failures. "

    "A quarantined record is not the same as a failed pipeline. "

    "The workflow can continue when the data quality process "
    "completes successfully and the reconciliation between total, "
    "valid, and quarantined records succeeds. "

    "If DQX reconciliation fails, the pipeline fails and Gold "
    "must not run. "

    "IMPORTANT: Clearly distinguish validation that happens "
    "before Gold from validation that happens inside or after "
    "the Gold pipeline. "

    "Do not describe Gold-specific checks such as Gold quality, "
    "key uniqueness, referential integrity, or dimensional model "
    "validation as pre-Gold validation unless the retrieved "
    "context explicitly says so. "

    "If the retrieved context does not contain enough information "
    "to answer a part of the question, omit that part or say that "
    "the available knowledge does not contain enough information. "

    "Keep the answer concise and focused on exactly what the "
    "user asked. "

    "Use Markdown when it improves readability."
)


# -------------------------------------------------------------------
# Agent
# -------------------------------------------------------------------

def create_agent() -> Agent:
    """
    Create the Retail Data Platform RAG agent.
    """

    return Agent(
        name="Retail Data Platform Assistant",
        instructions=AGENT_INSTRUCTIONS,
        model=LLM_ENDPOINT,
        tools=[
            search_retail_knowledge,
        ],
    )


# -------------------------------------------------------------------
# Non-streaming requests
# -------------------------------------------------------------------

@invoke()
async def invoke_handler(
    request: ResponsesAgentRequest,
) -> ResponsesAgentResponse:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(
            metadata={
                "mlflow.trace.session": session_id
            }
        )

    agent = create_agent()

    messages = normalize_history_items(
        [
            item.model_dump()
            for item in request.input
        ]
    )

    result = await Runner.run(
        agent,
        messages,
    )

    return ResponsesAgentResponse(
        output=[
            item.to_input_item()
            for item in result.new_items
        ]
    )


# -------------------------------------------------------------------
# Streaming requests
# -------------------------------------------------------------------

@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[
    ResponsesAgentStreamEvent,
    None,
]:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(
            metadata={
                "mlflow.trace.session": session_id
            }
        )

    agent = create_agent()

    messages = normalize_history_items(
        [
            item.model_dump()
            for item in request.input
        ]
    )

    result = Runner.run_streamed(
        agent,
        input=messages,
    )

    async for event in process_agent_stream_events(
        result.stream_events()
    ):
        yield event