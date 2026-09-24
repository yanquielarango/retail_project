import argparse

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


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--ai-schema", required=True)
    parser.add_argument("--volume-schema", required=True)
    parser.add_argument("--pdf-volume", required=True)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = SparkSession.getActiveSession()

    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    gold_schema = args.gold_schema
    ai_schema = args.ai_schema
    volume_schema = args.volume_schema
    pdf_volume = args.pdf_volume

    pdf_volume_path = (
        f"/Volumes/{catalog}/{volume_schema}/{pdf_volume}/pdf"
    )

    rag_table = f"{catalog}.{ai_schema}.rag_chunks"


    dim_product_df = spark.table(
        f"{catalog}.{gold_schema}.dim_product"
    )

    fact_sales_df = spark.table(
        f"{catalog}.{gold_schema}.fact_sales"
    )


    product_sales_summary_df = build_product_sales_summary(
        fact_sales_df
    )

    product_documents_df = build_product_documents(
        dim_product_df,
        product_sales_summary_df,
    )


    pdf_df = read_pdf_documents(
        spark,
        pdf_volume_path,
    )

    parsed_pdf_df = parse_pdf_documents(pdf_df)

    product_chunks_df = build_product_search_chunks(
        spark,
        product_documents_df,
    )

    pdf_chunks_df = build_pdf_search_chunks(
        spark,
        parsed_pdf_df,
    )


    rag_chunks_df = combine_search_chunks(
        product_chunks_df,
        pdf_chunks_df,
    )


    (
        rag_chunks_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .saveAsTable(rag_table)
    )


if __name__ == "__main__":
    main()
