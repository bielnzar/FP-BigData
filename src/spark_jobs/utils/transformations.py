# src/spark_jobs/utils/transformations.py
from pyspark.sql.functions import col, lower, regexp_replace, trim, split, explode
from pyspark.ml.feature import StopWordsRemover, Tokenizer, HashingTF, IDF # Contoh untuk NLP

def clean_text_column(df, text_column_name, new_column_name="cleaned_text"):
    """
    Membersihkan kolom teks: lowercase, hapus tanda baca, hapus spasi berlebih.
    """
    if text_column_name not in df.columns:
        print(f"Warning: Column '{text_column_name}' not found in DataFrame.")
        return df

    cleaned_df = df.withColumn(new_column_name, lower(col(text_column_name)))
    # Hapus tanda baca dasar (bisa diperluas)
    cleaned_df = cleaned_df.withColumn(new_column_name, regexp_replace(col(new_column_name), r"[^\w\s]", ""))
    # Hapus spasi berlebih
    cleaned_df = cleaned_df.withColumn(new_column_name, trim(regexp_replace(col(new_column_name), r"\s+", " ")))
    return cleaned_df

def tokenize_text(df, input_col="cleaned_text", output_col="tokens"):
    """ Tokenisasi teks menjadi kata-kata. """
    if input_col not in df.columns:
        print(f"Warning: Column '{input_col}' not found for tokenization.")
        return df
    tokenizer = Tokenizer(inputCol=input_col, outputCol=output_col)
    return tokenizer.transform(df)

def remove_stopwords(df, input_col="tokens", output_col="filtered_tokens"):
    """ Menghapus stopwords dari daftar token. """
    if input_col not in df.columns:
        print(f"Warning: Column '{input_col}' not found for stopword removal.")
        return df
    # Anda bisa menambahkan custom stopwords atau menggunakan bahasa lain
    remover = StopWordsRemover(inputCol=input_col, outputCol=output_col, caseSensitive=False)
    return remover.transform(df)

# Anda bisa menambahkan fungsi transformasi lain di sini:
# - Fungsi untuk casting tipe data
# - Fungsi untuk handling missing values
# - Fungsi untuk feature engineering spesifik
# - Fungsi untuk stemming/lemmatization (membutuhkan library tambahan seperti NLTK/spaCy via UDF atau Spark NLP)

if __name__ == "__main__":
    # Contoh penggunaan (membutuhkan SparkSession)
    from spark_session_manager import get_spark_session
    spark = get_spark_session("TransformationsTest")

    data = [
        (1, "  Ini Adalah Contoh TEKS dengan Tanda Baca! dan Spasi Berlebih.  "),
        (2, "Teks kedua yang cukup bersih."),
        (3, None) # Contoh teks null
    ]
    df = spark.createDataFrame(data, ["id", "original_text"])
    df.show(truncate=False)

    df_cleaned = clean_text_column(df, "original_text")
    df_cleaned.show(truncate=False)

    df_tokenized = tokenize_text(df_cleaned.na.drop(subset=["cleaned_text"]), "cleaned_text") # Drop NA sebelum tokenisasi
    df_tokenized.show(truncate=False)
    
    if "tokens" in df_tokenized.columns: # Pastikan kolom token ada
        df_stopwords_removed = remove_stopwords(df_tokenized, "tokens")
        df_stopwords_removed.show(truncate=False)

    spark.stop()