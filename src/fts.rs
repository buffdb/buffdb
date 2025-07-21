//! Full-text search implementation for BuffDB
//!
//! This module provides full-text search capabilities including tokenization,
//! stemming, and ranked search results using BM25 scoring.

use crate::index::IndexError;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Full-text search index
pub struct FtsIndex {
    name: String,
    /// Document frequency: term -> number of documents containing the term
    doc_freq: Arc<RwLock<HashMap<String, usize>>>,
    /// Inverted index: term -> set of (doc_id, term_frequency, positions)
    inverted_index: Arc<RwLock<HashMap<String, Vec<DocTermInfo>>>>,
    /// Document lengths: doc_id -> number of terms
    doc_lengths: Arc<RwLock<HashMap<String, usize>>>,
    /// Total number of documents
    doc_count: Arc<RwLock<usize>>,
    /// Average document length
    avg_doc_length: Arc<RwLock<f64>>,
    /// Tokenizer configuration
    tokenizer: Tokenizer,
}

#[derive(Debug, Clone)]
struct DocTermInfo {
    doc_id: String,
    term_freq: usize,
    positions: Vec<usize>,
}

/// Tokenizer for breaking text into searchable terms
#[derive(Debug, Clone)]
pub struct Tokenizer {
    /// Minimum token length
    min_length: usize,
    /// Maximum token length
    max_length: usize,
    /// Whether to convert to lowercase
    lowercase: bool,
    /// Stop words to ignore
    stop_words: HashSet<String>,
    /// Whether to apply stemming
    stemming: bool,
}

impl Default for Tokenizer {
    fn default() -> Self {
        let mut stop_words = HashSet::new();
        // Common English stop words
        for word in &[
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in",
            "is", "it", "its", "of", "on", "that", "the", "to", "was", "will", "with",
        ] {
            stop_words.insert(word.to_string());
        }

        Self {
            min_length: 2,
            max_length: 50,
            lowercase: true,
            stop_words,
            stemming: true,
        }
    }
}

impl Tokenizer {
    /// Tokenize text into terms
    pub fn tokenize(&self, text: &str) -> Vec<(String, usize)> {
        let mut tokens = Vec::new();
        let mut position = 0;

        // Simple whitespace and punctuation tokenization
        let words: Vec<&str> = text.split(|c: char| !c.is_alphanumeric()).collect();

        for word in words {
            if word.is_empty() {
                continue;
            }

            let mut token = word.to_string();

            // Apply lowercase
            if self.lowercase {
                token = token.to_lowercase();
            }

            // Check length constraints
            if token.len() < self.min_length || token.len() > self.max_length {
                continue;
            }

            // Skip stop words
            if self.stop_words.contains(&token) {
                continue;
            }

            // Apply simple stemming (Porter stemmer lite)
            if self.stemming {
                token = self.simple_stem(&token);
            }

            tokens.push((token, position));
            position += 1;
        }

        tokens
    }

    /// Simple stemming implementation (removes common suffixes)
    fn simple_stem(&self, word: &str) -> String {
        // Very basic stemming - just removes common English suffixes
        let suffixes = ["ing", "ed", "es", "s", "ly", "er", "est"];

        for suffix in &suffixes {
            if word.len() > suffix.len() + 3 && word.ends_with(suffix) {
                return word[..word.len() - suffix.len()].to_string();
            }
        }

        word.to_string()
    }
}

/// Search result with relevance score
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: String,
    pub score: f64,
    pub highlights: Vec<String>,
}

impl FtsIndex {
    /// Create a new full-text search index
    pub fn new(name: String) -> Self {
        Self {
            name,
            doc_freq: Arc::new(RwLock::new(HashMap::new())),
            inverted_index: Arc::new(RwLock::new(HashMap::new())),
            doc_lengths: Arc::new(RwLock::new(HashMap::new())),
            doc_count: Arc::new(RwLock::new(0)),
            avg_doc_length: Arc::new(RwLock::new(0.0)),
            tokenizer: Tokenizer::default(),
        }
    }

    /// Create with custom tokenizer
    pub fn with_tokenizer(name: String, tokenizer: Tokenizer) -> Self {
        Self {
            name,
            doc_freq: Arc::new(RwLock::new(HashMap::new())),
            inverted_index: Arc::new(RwLock::new(HashMap::new())),
            doc_lengths: Arc::new(RwLock::new(HashMap::new())),
            doc_count: Arc::new(RwLock::new(0)),
            avg_doc_length: Arc::new(RwLock::new(0.0)),
            tokenizer,
        }
    }

    /// Index a document
    pub fn index_document(&self, doc_id: String, content: &str) -> Result<(), IndexError> {
        let tokens = self.tokenizer.tokenize(content);

        // Count term frequencies and positions
        let mut term_positions: HashMap<String, Vec<usize>> = HashMap::new();
        for (term, position) in &tokens {
            term_positions
                .entry(term.clone())
                .or_insert_with(Vec::new)
                .push(*position);
        }

        // Update inverted index
        let mut inverted_index = self.inverted_index.write().unwrap();
        let mut doc_freq = self.doc_freq.write().unwrap();
        let mut doc_lengths = self.doc_lengths.write().unwrap();

        // Remove old document if it exists
        if doc_lengths.contains_key(&doc_id) {
            self.remove_document_internal(&doc_id, &mut inverted_index, &mut doc_freq);
        }

        // Add new document
        for (term, positions) in term_positions {
            let term_freq = positions.len();

            let doc_info = DocTermInfo {
                doc_id: doc_id.clone(),
                term_freq,
                positions,
            };

            // Update inverted index
            inverted_index
                .entry(term.clone())
                .or_insert_with(Vec::new)
                .push(doc_info);

            // Update document frequency
            *doc_freq.entry(term).or_insert(0) += 1;
        }

        // Update document length
        doc_lengths.insert(doc_id, tokens.len());

        // Update statistics
        let mut doc_count = self.doc_count.write().unwrap();
        *doc_count = doc_lengths.len();

        let mut avg_length = self.avg_doc_length.write().unwrap();
        *avg_length = doc_lengths.values().sum::<usize>() as f64 / *doc_count as f64;

        Ok(())
    }

    /// Remove a document from the index
    pub fn remove_document(&self, doc_id: &str) -> Result<(), IndexError> {
        let mut inverted_index = self.inverted_index.write().unwrap();
        let mut doc_freq = self.doc_freq.write().unwrap();
        let mut doc_lengths = self.doc_lengths.write().unwrap();

        self.remove_document_internal(doc_id, &mut inverted_index, &mut doc_freq);
        doc_lengths.remove(doc_id);

        // Update statistics
        let mut doc_count = self.doc_count.write().unwrap();
        *doc_count = doc_lengths.len();

        if *doc_count > 0 {
            let mut avg_length = self.avg_doc_length.write().unwrap();
            *avg_length = doc_lengths.values().sum::<usize>() as f64 / *doc_count as f64;
        }

        Ok(())
    }

    fn remove_document_internal(
        &self,
        doc_id: &str,
        inverted_index: &mut HashMap<String, Vec<DocTermInfo>>,
        doc_freq: &mut HashMap<String, usize>,
    ) {
        // Remove document from all term posting lists
        let mut empty_terms = Vec::new();

        for (term, docs) in inverted_index.iter_mut() {
            docs.retain(|doc| doc.doc_id != doc_id);
            if docs.is_empty() {
                empty_terms.push(term.clone());
            }
        }

        // Remove empty terms
        for term in empty_terms {
            inverted_index.remove(&term);
            doc_freq.remove(&term);
        }
    }

    /// Search for documents matching the query
    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let query_terms = self.tokenizer.tokenize(query);
        if query_terms.is_empty() {
            return Vec::new();
        }

        let inverted_index = self.inverted_index.read().unwrap();
        let doc_freq = self.doc_freq.read().unwrap();
        let doc_lengths = self.doc_lengths.read().unwrap();
        let doc_count = *self.doc_count.read().unwrap();
        let avg_doc_length = *self.avg_doc_length.read().unwrap();

        // Calculate BM25 scores
        let mut scores: HashMap<String, f64> = HashMap::new();
        let mut doc_terms: HashMap<String, HashSet<String>> = HashMap::new();

        // BM25 parameters
        let k1 = 1.2;
        let b = 0.75;

        for (term, _) in &query_terms {
            if let Some(docs) = inverted_index.get(term) {
                let df = doc_freq.get(term).unwrap_or(&0);
                let idf = ((doc_count as f64 - *df as f64 + 0.5) / (*df as f64 + 0.5)).ln();

                for doc_info in docs {
                    let doc_len = doc_lengths.get(&doc_info.doc_id).unwrap_or(&0);
                    let normalized_len = *doc_len as f64 / avg_doc_length;

                    // BM25 scoring
                    let tf = doc_info.term_freq as f64;
                    let score =
                        idf * (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b * normalized_len));

                    *scores.entry(doc_info.doc_id.clone()).or_insert(0.0) += score;
                    doc_terms
                        .entry(doc_info.doc_id.clone())
                        .or_insert_with(HashSet::new)
                        .insert(term.clone());
                }
            }
        }

        // Sort by score
        let mut results: Vec<_> = scores
            .into_iter()
            .map(|(doc_id, score)| {
                let terms = doc_terms.get(&doc_id).unwrap();
                let highlights = self.generate_highlights(&doc_id, terms);
                SearchResult {
                    doc_id,
                    score,
                    highlights,
                }
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        results.truncate(limit);

        results
    }

    /// Search for documents containing exact phrase
    pub fn phrase_search(&self, phrase: &str, limit: usize) -> Vec<SearchResult> {
        let phrase_terms = self.tokenizer.tokenize(phrase);
        if phrase_terms.is_empty() {
            return Vec::new();
        }

        let inverted_index = self.inverted_index.read().unwrap();

        // Find documents containing all terms
        let mut candidate_docs: Option<HashSet<String>> = None;

        for (term, _) in &phrase_terms {
            if let Some(docs) = inverted_index.get(term) {
                let doc_ids: HashSet<String> = docs.iter().map(|d| d.doc_id.clone()).collect();

                candidate_docs = match candidate_docs {
                    None => Some(doc_ids),
                    Some(existing) => Some(existing.intersection(&doc_ids).cloned().collect()),
                };
            } else {
                // Term not found, no results
                return Vec::new();
            }
        }

        let candidates = match candidate_docs {
            Some(docs) => docs,
            None => return Vec::new(),
        };

        // Check phrase proximity in candidate documents
        let mut results = Vec::new();

        for doc_id in candidates {
            if self.contains_phrase(&doc_id, &phrase_terms, &inverted_index) {
                results.push(SearchResult {
                    doc_id,
                    score: 1.0, // Simple scoring for phrase search
                    highlights: vec![phrase.to_string()],
                });
            }
        }

        results.truncate(limit);
        results
    }

    fn contains_phrase(
        &self,
        doc_id: &str,
        phrase_terms: &[(String, usize)],
        inverted_index: &HashMap<String, Vec<DocTermInfo>>,
    ) -> bool {
        // Get positions for each term in the document
        let mut term_positions: Vec<Vec<usize>> = Vec::new();

        for (term, _) in phrase_terms {
            if let Some(docs) = inverted_index.get(term) {
                if let Some(doc_info) = docs.iter().find(|d| d.doc_id == doc_id) {
                    term_positions.push(doc_info.positions.clone());
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check if terms appear consecutively
        if term_positions.is_empty() {
            return false;
        }

        for &pos in &term_positions[0] {
            let mut found = true;
            for i in 1..term_positions.len() {
                if !term_positions[i].contains(&(pos + i)) {
                    found = false;
                    break;
                }
            }
            if found {
                return true;
            }
        }

        false
    }

    fn generate_highlights(&self, _doc_id: &str, terms: &HashSet<String>) -> Vec<String> {
        // Simple highlight generation - just return the matched terms
        terms.iter().cloned().collect()
    }
}

/// Full-text search manager
pub struct FtsManager {
    indexes: Arc<RwLock<HashMap<String, FtsIndex>>>,
}

impl FtsManager {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new FTS index
    pub fn create_index(&self, name: String) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.contains_key(&name) {
            return Err(IndexError::IndexAlreadyExists { name });
        }

        indexes.insert(name.clone(), FtsIndex::new(name));
        Ok(())
    }

    /// Create a new FTS index with custom tokenizer
    pub fn create_index_with_tokenizer(
        &self,
        name: String,
        tokenizer: Tokenizer,
    ) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.contains_key(&name) {
            return Err(IndexError::IndexAlreadyExists { name });
        }

        indexes.insert(
            name.clone(),
            FtsIndex::with_tokenizer(name.clone(), tokenizer),
        );
        Ok(())
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Option<FtsIndex> {
        let indexes = self.indexes.read().unwrap();
        indexes.get(name).cloned()
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> Result<(), IndexError> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.remove(name).is_none() {
            return Err(IndexError::IndexNotFound {
                name: name.to_string(),
            });
        }

        Ok(())
    }
}

// Implement Clone for FtsIndex to satisfy the get_index requirement
impl Clone for FtsIndex {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            doc_freq: Arc::clone(&self.doc_freq),
            inverted_index: Arc::clone(&self.inverted_index),
            doc_lengths: Arc::clone(&self.doc_lengths),
            doc_count: Arc::clone(&self.doc_count),
            avg_doc_length: Arc::clone(&self.avg_doc_length),
            tokenizer: self.tokenizer.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("The quick brown fox jumps over the lazy dog");

        // Should filter out stop words like "the", "over"
        assert!(!tokens.iter().any(|(t, _)| t == "the"));
        assert!(tokens.iter().any(|(t, _)| t == "quick"));
        assert!(tokens.iter().any(|(t, _)| t == "brown"));
        assert!(tokens.iter().any(|(t, _)| t == "fox"));
    }

    #[test]
    fn test_basic_search() {
        let index = FtsIndex::new("test".to_string());

        // Index some documents
        index
            .index_document(
                "doc1".to_string(),
                "The quick brown fox jumps over the lazy dog",
            )
            .unwrap();
        index
            .index_document("doc2".to_string(), "The lazy cat sleeps under the warm sun")
            .unwrap();
        index
            .index_document(
                "doc3".to_string(),
                "Quick brown rabbits hop through the garden",
            )
            .unwrap();

        // Search for "brown"
        let results = index.search("brown", 10);
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.doc_id == "doc1"));
        assert!(results.iter().any(|r| r.doc_id == "doc3"));

        // Search for "lazy"
        let results = index.search("lazy", 10);
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.doc_id == "doc1"));
        assert!(results.iter().any(|r| r.doc_id == "doc2"));
    }

    #[test]
    fn test_phrase_search() {
        let index = FtsIndex::new("test".to_string());

        index
            .index_document(
                "doc1".to_string(),
                "The quick brown fox jumps over the lazy dog",
            )
            .unwrap();
        index
            .index_document("doc2".to_string(), "The brown quick fox is not here")
            .unwrap();

        // Search for exact phrase
        let results = index.phrase_search("quick brown", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "doc1");

        // Search for phrase that doesn't exist
        let results = index.phrase_search("brown quick", 10);
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_stemming() {
        let tokenizer = Tokenizer::default();

        // Test basic stemming
        assert_eq!(tokenizer.simple_stem("running"), "runn");
        assert_eq!(tokenizer.simple_stem("jumped"), "jump");
        assert_eq!(tokenizer.simple_stem("flies"), "fli");
        assert_eq!(tokenizer.simple_stem("quickly"), "quick");
    }
}
