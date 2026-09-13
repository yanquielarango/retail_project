from pyspark.sql import DataFrame, SparkSession


def read_pdf_documents(
    spark: SparkSession,
    volume_path: str,
) -> DataFrame:
    """
    Read PDF files from a Unity Catalog Volume as binary data.
    """

    return (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.pdf")
        .load(volume_path)
        .select("path", "content")
    )


def parse_pdf_documents(
    pdf_df: DataFrame,
) -> DataFrame:
    """
    Parse PDF documents using Databricks ai_parse_document.

    The parsed_document column is a VARIANT containing document
    structure such as text, tables, titles, sections and page data.
    """

    return pdf_df.selectExpr(
        "path",
        "ai_parse_document(content, map('version', '2.0')) AS parsed_document",
    )