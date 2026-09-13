from pdf_documents import (
    parse_pdf_documents,
    read_pdf_documents,
)
from product_documents import (
    build_product_documents,
    build_product_sales_summary,
)
from pyspark.sql import SparkSession
from search_documents import (
    build_pdf_search_chunks,
    build_product_search_chunks,
    combine_search_chunks,
)

CATALOG = "dbr_dev"
GOLD_SCHEMA = "retail_gold"
AI_SCHEMA = "retail_ai"
VOLUME_SCHEMA = "volumes"
PDF_VOLUME = "retail_knowledge"

PDF_VOLUME_PATH = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{PDF_VOLUME}/pdf"
RAG_TABLE = f"{CATALOG}.{AI_SCHEMA}.rag_chunks"


def main() -> None:
    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    # ---------------------------------------------------------
    # Gold tables
    # ---------------------------------------------------------

    dim_product_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_product")
    fact_sales_df = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")

    # ---------------------------------------------------------
    # Structured product documents
    # ---------------------------------------------------------

    product_sales_summary_df = build_product_sales_summary(fact_sales_df)

    product_documents_df = build_product_documents(
        dim_product_df,
        product_sales_summary_df,
    )

    # ---------------------------------------------------------
    # PDF documents
    # ---------------------------------------------------------

    pdf_df = read_pdf_documents(spark, PDF_VOLUME_PATH)
    parsed_pdf_df = parse_pdf_documents(pdf_df)

    # ---------------------------------------------------------
    # Semantic chunks
    # ---------------------------------------------------------

    product_chunks_df = build_product_search_chunks(
        spark,
        product_documents_df,
    )

    pdf_chunks_df = build_pdf_search_chunks(
        spark,
        parsed_pdf_df,
    )

    # ---------------------------------------------------------
    # Unified RAG dataset
    # ---------------------------------------------------------

    rag_chunks_df = combine_search_chunks(
        product_chunks_df,
        pdf_chunks_df,
    )

    # ---------------------------------------------------------
    # Write Delta source table for AI Search
    # ---------------------------------------------------------

    (
        rag_chunks_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(RAG_TABLE)
    )


if __name__ == "__main__":
    main()