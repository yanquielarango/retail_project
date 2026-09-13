from pyspark.sql import DataFrame, SparkSession


def build_pdf_search_chunks(
    spark: SparkSession,
    parsed_pdf_df: DataFrame,
) -> DataFrame:
    """
    Convert parsed PDFs into semantic chunks ready for AI Search.
    """
    parsed_pdf_df.createOrReplaceTempView("_rag_parsed_pdf_documents")

    return spark.sql(
        """
        WITH prepared AS (
            SELECT
                path,
                ai_prep_search(parsed_document, map('version', '2.0')) AS result
            FROM _rag_parsed_pdf_documents
        )
        SELECT
            concat('pdf_', sha2(path, 256), '_', cast(chunk.value:chunk_position AS STRING)) AS chunk_id,
            cast(chunk.value:chunk_position AS INT) AS chunk_position,
            cast(chunk.value:chunk_to_retrieve AS STRING) AS chunk_to_retrieve,
            cast(chunk.value:chunk_to_embed AS STRING) AS chunk_to_embed,
            cast(chunk.value:metadata AS STRING) AS metadata,
            path AS source_uri,
            'pdf' AS document_type,
            cast(NULL AS STRING) AS product_id,
            cast(NULL AS STRING) AS category,
            cast(NULL AS STRING) AS product_segment
        FROM prepared,
        LATERAL variant_explode(prepared.result:document.contents) AS chunk
        """
    )


def build_product_search_chunks(
    spark: SparkSession,
    product_documents_df: DataFrame,
) -> DataFrame:
    """
    Convert product text documents into semantic chunks
    ready for AI Search.
    """
    product_documents_df.createOrReplaceTempView("_rag_product_documents")

    return spark.sql(
        """
        WITH prepared AS (
            SELECT
                document_id,
                document_type,
                source,
                product_id,
                category,
                product_segment,
                ai_prep_search(content, map('version', '2.0')) AS result
            FROM _rag_product_documents
        )
        SELECT
            concat(document_id, '_', cast(chunk.value:chunk_position AS STRING)) AS chunk_id,
            cast(chunk.value:chunk_position AS INT) AS chunk_position,
            cast(chunk.value:chunk_to_retrieve AS STRING) AS chunk_to_retrieve,
            cast(chunk.value:chunk_to_embed AS STRING) AS chunk_to_embed,
            cast(chunk.value:metadata AS STRING) AS metadata,
            source AS source_uri,
            document_type,
            product_id,
            category,
            product_segment
        FROM prepared,
        LATERAL variant_explode(prepared.result:document.contents) AS chunk
        """
    )


def combine_search_chunks(
    product_chunks_df: DataFrame,
    pdf_chunks_df: DataFrame,
) -> DataFrame:
    """
    Combine structured Gold knowledge and PDF knowledge
    into one RAG source dataset.
    """
    return product_chunks_df.unionByName(
        pdf_chunks_df,
        allowMissingColumns=True,
    )