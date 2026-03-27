# Databricks notebook source
# MAGIC %pip install sentence-transformers faiss-cpu -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("primeins.gold.dim_policy")

# COMMAND ----------

def policy_to_text(row):
    return f"""
Policy Number: {row['policy_number']}

This policy was issued on {row['policy_bind_date']} in the state of {row['policy_state']}.

Coverage Details:
- Combined Single Limit (CSL): {row['policy_csl']}
- Deductible Amount: {row['policy_deductable']}
- Umbrella Coverage Limit: {row['umbrella_limit']}

Financial Details:
- Annual Premium: {row['policy_annual_premium']}

Associations:
- Car ID linked to this policy: {row['car_id']}
- Customer ID owning this policy: {row['customer_id']}

Summary:
This is a {row['policy_csl']} policy in {row['policy_state']} with a deductible of {row['policy_deductable']} and an annual premium of {row['policy_annual_premium']}.
"""

# COMMAND ----------

# DBTITLE 1,Cell 4
pdf = df.toPandas()
docs = pdf.apply(lambda r: {
    "policy_number": r["policy_number"],
    "text": policy_to_text(r)
}, axis=1).tolist()

# COMMAND ----------

def chunk_text(text, chunk_size=200):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunks.append(" ".join(words[i:i+chunk_size]))
    return chunks

chunked_docs = []
for d in docs:
    chunks = chunk_text(d["text"])
    for c in chunks:
        chunked_docs.append({
            "policy_number": d["policy_number"],
            "chunk": c
        })

len(chunked_docs)

# COMMAND ----------

from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")

texts = [c["chunk"] for c in chunked_docs]
embeddings = model.encode(texts, show_progress_bar=True)

embedding_matrix = np.array(embeddings)
embedding_matrix.shape

# COMMAND ----------

import faiss

dimension = embedding_matrix.shape[1]

index = faiss.IndexFlatL2(dimension)
index.add(embedding_matrix)

print("Total vectors indexed:", index.ntotal)

# COMMAND ----------

def retrieve(query, top_k=5):
    query_vec = model.encode([query])
    distances, indices = index.search(np.array(query_vec), top_k)
    
    results = []
    for i, idx in enumerate(indices[0]):
        results.append({
            "policy_number": chunked_docs[idx]["policy_number"],
            "chunk": chunked_docs[idx]["chunk"],
            "distance": float(distances[0][i])
        })
    return results

# COMMAND ----------

def generate_answer(query, retrieved_chunks):
    context = "\n\n".join([c["chunk"] for c in retrieved_chunks])
    policy_ids = list(set([c["policy_number"] for c in retrieved_chunks]))

    prompt = f"""
Answer the question using the policy data below.
Always cite policy numbers.

Context:
{context}

Question:
{query}
"""

    # Placeholder (replace with real LLM call)
    answer = f"Answer based on policies {policy_ids}: {context[:200]}..."

    return answer, policy_ids

# COMMAND ----------

def compute_confidence(results):
    distances = [r["distance"] for r in results]
    score = 1 / (1 + np.mean(distances))
    return float(score)

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime

def log_query(question, answer, confidence, policies):
    log_df = spark.createDataFrame([Row(
        question=question,
        answer=answer,
        confidence_score=confidence,
        source_policies=",".join(map(str, policies)),
        timestamp=datetime.now()
    )])

    log_df.write.mode("append").saveAsTable("primeins.gold.rag_query_history")

# COMMAND ----------

def ask_question(query):
    retrieved = retrieve(query, top_k=5)
    answer, policies = generate_answer(query, retrieved)
    confidence = compute_confidence(retrieved)

    log_query(query, answer, confidence, policies)

    return {
        "question": query,
        "answer": answer,
        "policies": policies,
        "confidence": confidence
    }

# COMMAND ----------

questions = [
    # 1. Specific lookup
    "What is the annual premium for policy number 743092?",

    # 2. Filter query
    "How many policies are in Ohio (OH)",

    # 3. Multi-Attribute Lookup
    "What policies have a 250/500 coverage limit with a $1,000 deductible?",

    # 4. Aggregation-style
    "What is the average premium across policies?",

    # 5. Semantic query
    "Find policies with high coverage but low premium"
]

results = [ask_question(q) for q in questions]

for r in results:
    print("--------------------------------------------------")
    print("Q:", r["question"])
    print("A:", r["answer"])
    print("Policies:", r["policies"])
    print("Confidence:", r["confidence"])

# COMMAND ----------

spark.table("primeins.gold.rag_query_history").display()

# COMMAND ----------

