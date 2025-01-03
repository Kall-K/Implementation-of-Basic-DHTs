from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import hashlib


class LSH:
    def __init__(self, num_bands=10, num_rows=5):
        """
        Initialize LSH with the number of bands and rows per band.
        :param num_bands: Number of bands for LSH.
        :param num_rows: Number of rows per band (controls locality precision).
        """
        self.num_bands = num_bands
        self.num_rows = num_rows
        self.hash_tables = [{} for _ in range(num_bands)]
        self.documents = []

    def _hash(self, vector):
        """
        Hash a vector using a stable hashing function (MD5).
        :param vector: A numpy array representing the signature.
        :return: List of hash values for each band.
        """
        band_hashes = []
        for i in range(self.num_bands):
            band = vector[i * self.num_rows : (i + 1) * self.num_rows]
            hash_value = hashlib.md5(band.tobytes()).hexdigest()
            band_hashes.append(hash_value)
        return band_hashes

    def add_document(self, doc_vector):
        """
        Add a document to the LSH index.
        :param doc_vector: Vectorized document signature.
        """
        self.documents.append(doc_vector)
        band_hashes = self._hash(doc_vector)
        for i, band_hash in enumerate(band_hashes):
            if band_hash not in self.hash_tables[i]:
                self.hash_tables[i][band_hash] = []
            self.hash_tables[i][band_hash].append(len(self.documents) - 1)

    def find_similar_pairs(self, N=5):
        """
        Find the top N most similar pairs of documents using LSH.

        :param N: Number of top pairs to return.
        :return: List of tuples containing (doc1_index, doc2_index, similarity_score).
        """
        pairs = set()
        for table in self.hash_tables:
            for bucket in table.values():
                if len(bucket) > 1:
                    for i in range(len(bucket)):
                        for j in range(i + 1, len(bucket)):
                            pairs.add((bucket[i], bucket[j]))

        # Compute similarities for unique pairs
        similarities = []
        for doc1, doc2 in pairs:
            sim = cosine_similarity([self.documents[doc1]], [self.documents[doc2]])[0][0] 
            similarities.append((doc1, doc2, sim))

        # Sort by similarity score
        similarities = sorted(similarities, key=lambda x: x[2], reverse=True)
        return similarities[:N]

    def find_similar_docs(self, similar_pairs, original_docs_texts, N):
        """
        Display the top N most similar documents from the similar pairs list.

        :param similar_pairs: List of similar pairs with similarity scores.
        :param original_documents: List of original document texts.
        """
        # Use a set to store indices of similar documents to avoid duplicates
        similar_docs_indices = set()
        for doc1, doc2, _ in similar_pairs:
            similar_docs_indices.add(doc1)
            similar_docs_indices.add(doc2)

        # Convert indices to document texts
        similar_docs_text = [original_docs_texts[idx] for idx in similar_docs_indices]
        
        return similar_docs_text[:N]


# Example Usage
if __name__ == "__main__":
    # Sample documents
    documents = [
        "This is a sample document.",
        "This document is another example.",
        "Completely unrelated text here.",
        "Yet another document with sample content.",
    ]

    # Preprocess and vectorize documents
    vectorizer = TfidfVectorizer()
    doc_vectors = vectorizer.fit_transform(documents).toarray()

    # Initialize and populate LSH
    lsh = LSH(num_bands=4, num_rows=5)
    for vector in doc_vectors:
        lsh.add_document(vector)

    # Find the top N most similar pairs
    N=3
    similar_pairs = lsh.find_similar_pairs(N)

    # Display a list of the top N similar documents
    similar_docs = lsh.find_similar_docs(similar_pairs, documents, N)

    print(f"\nThe {N} Most Similar Reviews:\n")
    for i, doc in enumerate(similar_docs, 1):
        print(f"{i}. {doc}\n")
